% Poolboy by Devin Torres <devin@devintorres.com>

-module(poolboy).
-behaviour(gen_fsm).

-export([checkout/1, checkin/2, start_link/1, init/1, ready/2, ready/3,
         overflow/2, overflow/3, full/2, full/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {workers, worker_sup, waiting=queue:new(), monitors=[],
                size=5, overflow=0, max_overflow=10, checkout_blocks=true}).

checkin(Pool, Worker) ->
    gen_fsm:send_event(Pool, {checkin, Worker}).

checkout(Pool) ->
    gen_fsm:sync_send_event(Pool, checkout).

start_link(Args) ->
    case proplists:get_value(name, Args) of
        undefined ->
            gen_fsm:start_link(?MODULE, Args, []);
        Name ->
            gen_fsm:start_link(Name, ?MODULE, Args, [])
    end.

init(Args) ->
    process_flag(trap_exit, true),
    init(Args, #state{}).

init([{worker_module, Mod} | Rest], State) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, Rest),
    init(Rest, State#state{worker_sup=Sup});
init([{size, PoolSize} | Rest], State) ->
    init(Rest, State#state{size=PoolSize});
init([{max_overflow, MaxOverflow} | Rest], State) ->
    init(Rest, State#state{max_overflow=MaxOverflow});
init([{checkout_blocks, CheckoutBlocks} | Rest], State) ->
    init(Rest, State#state{checkout_blocks=CheckoutBlocks});
init([_ | Rest], State) ->
    init(Rest, State);
init([], #state{size=Size, worker_sup=Sup}=State) ->
    Workers = prepopulate(Size, Sup),
    {ok, ready, State#state{workers=Workers}}.

ready({checkin, Pid}, State) ->
    Workers = queue:in(Pid, State#state.workers),
    Monitors = case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref}, Left} -> erlang:demonitor(Ref), Left;
        false -> []
    end,
    {next_state, ready, State#state{workers=Workers, monitors=Monitors}};
ready(_Event, State) ->
    {next_state, ready, State}.

ready(checkout, {FromPid, _} = From, #state{workers=Workers, worker_sup=Sup,
                                  max_overflow=MaxOverflow,
                                  checkout_blocks=Blocks}=State) ->
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            Monitors = [{Pid, Ref} | State#state.monitors],
            {reply, Pid, ready, State#state{workers=Left,
                                            monitors=Monitors}};
        {empty, Empty} when MaxOverflow > 0 ->
            {Pid, Ref} = new_worker(Sup, FromPid),
            Monitors = [{Pid, Ref} | State#state.monitors],
            {reply, Pid, overflow, State#state{workers=Empty,
                                               monitors=Monitors,
                                               overflow=1}};
        {empty, Empty}  when Blocks == false ->
            %% don't block the calling process
            {reply, full, full, State};
        {empty, Empty} ->
            Waiting = State#state.waiting,
            {next_state, full, State#state{workers=Empty,
                                           waiting=queue:in(From, Waiting)}}
    end;
ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

overflow({checkin, Pid}, #state{overflow=1}=State) ->
    dismiss_worker(Pid),
    {next_state, ready, State#state{overflow=0}};
overflow({checkin, Pid}, #state{overflow=Overflow}=State) ->
    dismiss_worker(Pid),
    {next_state, overflow, State#state{overflow=Overflow-1}};
overflow(_Event, State) ->
    {next_state, overflow, State}.

overflow(checkout, From, #state{overflow=Overflow,
         max_overflow=MaxOverflow}=State) when Overflow >= MaxOverflow ->
    Waiting = State#state.waiting,
    {next_state, full, State#state{waiting=queue:in(From, Waiting)}};
overflow(checkout, {From, _}, #state{worker_sup=Sup,
                                     overflow=Overflow}=State) ->
    {Pid, Ref} = new_worker(Sup, From),
    Monitors = [{Pid, Ref} | State#state.monitors],
    {reply, Pid, overflow, State#state{monitors=Monitors,
                                       overflow=Overflow+1}};
overflow(_Event, _From, State) ->
    {reply, ok, overflow, State}.

full({checkin, Pid}, #state{waiting=Waiting, max_overflow=MaxOverflow}=State) ->
    case queue:out(Waiting) of
        {{value, {FromPid, _} = From}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            Monitors = [{FromPid, Ref} | State#state.monitors],
            gen_fsm:reply(From, Pid),
            {next_state, full, State#state{waiting=Left,
                                           monitors=Monitors}};
        {empty, Empty} when MaxOverflow < 1 ->
            Workers = queue:in(Pid, State#state.workers),
            Monitors = case lists:keytake(Pid, 1, State#state.monitors) of
                {value, {_, Ref}, Left} -> erlang:demonitor(Ref), Left;
                false -> []
            end,
            {next_state, ready, State#state{waiting=Empty, workers=Workers,
                    monitors=Monitors}};
        {empty, Empty} ->
            dismiss_worker(Pid),
            {next_state, overflow, State#state{waiting=Empty}}
    end;
full(_Event, State) ->
    {next_state, full, State}.

full(checkout, From, #state{checkout_blocks=false}=State) ->
    {reply, full, full, State};
full(checkout, From, #state{waiting=Waiting}=State) ->
    {next_state, full, State#state{waiting=queue:in(From, Waiting)}};
full(_Event, _From, State) ->
    {reply, ok, full, State}.

handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
  Reply = {error, invalid_message},
  {reply, Reply, StateName, State}.

handle_info({'DOWN', Ref, _, _, _}, StateName, State) ->
    case lists:keytake(Ref, 2, State#state.monitors) of
        {value, {Pid, _}, _} -> dismiss_worker(Pid);
        false -> false
    end,
    {next_state, StateName, State};
handle_info({'EXIT', Pid, _}, StateName, #state{worker_sup=Sup,
                                                overflow=Overflow,
                                                max_overflow=MaxOverflow}=State) ->
    Monitors = case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref}, Left} -> erlang:demonitor(Ref), Left;
        false -> []
    end,
    case StateName of
        ready ->
            W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
            {next_state, ready, State#state{workers=queue:in(new_worker(Sup), W),
                                            monitors=Monitors}};
        overflow when Overflow =< 1 ->
            {next_state, ready, State#state{monitors=Monitors, overflow=0}};
        overflow ->
            {next_state, overflow, State#state{monitors=Monitors,
                                               overflow=Overflow-1}};
        full when Overflow =< MaxOverflow ->
            {next_state, overflow, State#state{monitors=Monitors,
                                               overflow=Overflow-1}};
        full ->
            {next_state, full, State#state{monitors=Monitors,
                                           overflow=Overflow-1}}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) -> ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    link(Pid),
    Pid.

new_worker(Sup, From) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, From),
    {Pid, Ref}.

dismiss_worker(Pid) ->
    unlink(Pid),
    Pid ! stop.

prepopulate(0, _Sup) ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).
