% Copyright (c) 2010, Devin Torres <devin@devintorres.com>

-module(poolboy).
-behaviour(gen_fsm).

-export([checkout/1, checkin/2, start_link/1, init/1, ready/2, ready/3,
         overflow/2, overflow/3, full/2, full/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {workers, worker_sup, waiting=queue:new(),
                size=5, overflow=0, max_overflow=10}).

checkin(Pool, Worker) ->
    gen_fsm:send_event(Pool, {checkin, Worker}).

checkout(Pool) ->
    gen_fsm:sync_send_event(Pool, checkout).

start_link(Args) ->
    Name = proplists:get_value(name, Args),
    gen_fsm:start_link(Name, ?MODULE, Args, []).

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
init([_ | Rest], State) ->
    init(Rest, State);
init([], #state{size=Size, worker_sup=Sup}=State) ->
    Workers = prepopulate(Size, Sup),
    {ok, ready, State#state{workers=Workers}}.

ready({checkin, Pid}, #state{workers=Workers}=State) ->
    {next_state, ready, State#state{workers=queue:in(Pid, Workers)}};
ready(_Event, State) ->
    {next_state, ready, State}.

ready(checkout, _From, #state{workers=Workers, worker_sup=Sup}=State) ->
    case queue:out(Workers) of
        {{value, Reply}, Remaining} ->
            {reply, Reply, ready, State#state{workers=Remaining}};
        {empty, Empty} ->
            Pid = new_worker(Sup),
            {reply, Pid, overflow, State#state{workers=Empty, overflow=1}}
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
overflow(checkout, _From, #state{overflow=Overflow, worker_sup=Sup}=State) ->
    {reply, new_worker(Sup), overflow, State#state{overflow=Overflow+1}};
overflow(_Event, _From, State) ->
    {reply, ok, overflow, State}.

full({checkin, Pid}, #state{waiting=Waiting}=State) ->
    case queue:out(Waiting) of
        {{value, FromPid}, Remaining} ->
            gen_fsm:reply(FromPid, Pid),
            {next_state, full, State#state{waiting=Remaining}};
        {empty, Empty} ->
            dismiss_worker(Pid),
            {next_state, overflow, State#state{waiting=Empty}}
    end;
full(_Event, State) ->
    {next_state, full, State}.

full(checkout, From, #state{waiting=Waiting}=State) ->
    {next_state, full, State#state{waiting=queue:in(From, Waiting)}};
full(_Event, _From, State) ->
    {reply, ok, full, State}.

handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
  Reply = {error, invalid_message},
  {reply, Reply, StateName, State}.

handle_info({'EXIT', Pid, _}, ready, #state{worker_sup=Sup}=State) ->
    Workers = queue:filter(fun (W) -> W =/= Pid end, State#state.workers),
    {next_state, ready, State#state{workers=queue:in(new_worker(Sup), Workers)}};
handle_info({'EXIT', Sup, Msg}, _, #state{worker_sup=Sup}=State) ->
    {stop, Msg, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) -> ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    link(Pid),
    Pid.

dismiss_worker(Pid) -> gen_server:cast(Pid, stop).

prepopulate(0, _Sup) ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).
