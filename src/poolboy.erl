%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_fsm).

-export([start_link/1, checkout/1, checkout/2, checkout/3, checkin/2, stop/1]).
-export([init/1, ready/2, ready/3, overflow/2, overflow/3, full/2, full/3,
         handle_event/3, handle_sync_event/4, handle_info/3, terminate/3,
         code_change/4]).

-define(TIMEOUT, 5000).
-define(DEFAULT_INIT_FUN, fun(Worker) -> {ok, Worker} end).
-define(DEFAULT_STOP_FUN, fun(Worker) -> Worker ! stop end).

-record(state, {
    workers :: queue(),
    worker_sup :: pid(),
    waiting = queue:new() :: queue(),
    monitors = [] :: list(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    worker_init = ?DEFAULT_INIT_FUN :: fun((Worker :: pid()) -> {ok, pid()}),
    worker_stop = ?DEFAULT_STOP_FUN :: fun((Worker :: pid()) -> stop)
}).

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    gen_fsm:sync_send_event(Pool, {checkout, Block, Timeout}, Timeout).

-spec checkin(Pool :: node(), Worker :: pid()) -> ok.
checkin(Pool, Worker) ->
    gen_fsm:send_event(Pool, {checkin, Worker}).

start_link(Args) ->
    case proplists:get_value(name, Args) of
        undefined ->
            gen_fsm:start_link(?MODULE, Args, []);
        Name ->
            gen_fsm:start_link(Name, ?MODULE, Args, [])
    end.

stop(Pool) ->
    gen_fsm:sync_send_all_state_event(Pool, stop).

init(Args) ->
    process_flag(trap_exit, true),
    init(Args, #state{}).

init([{worker_module, Mod} | Rest], State) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, Rest),
    init(Rest, State#state{worker_sup=Sup});
init([{size, Size} | Rest], State) ->
    init(Rest, State#state{size=Size});
init([{max_overflow, MaxOverflow} | Rest], State) ->
    init(Rest, State#state{max_overflow=MaxOverflow});
init([{init_fun, InitFun} | Rest], State) when is_function(InitFun) ->
    init(Rest, State#state{worker_init=InitFun});
init([{stop_fun, StopFun} | Rest], State) when is_function(StopFun) ->
    init(Rest, State#state{worker_stop=StopFun});
init([_ | Rest], State) ->
    init(Rest, State);
init([], #state{size=Size, worker_sup=Sup, worker_init=InitFun,
        max_overflow=MaxOverflow}=State) ->
    Workers = prepopulate(Size, Sup, InitFun),
    StartState = case queue:len(Workers) of
        0 when MaxOverflow =:= 0 -> full;
        0 -> overflow;
        _ -> ready
    end,
    {ok, StartState, State#state{workers=Workers}}.

ready({checkin, Pid}, State) ->
    case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref}, Monitors} ->
            erlang:demonitor(Ref),
            Workers = queue:in(Pid, State#state.workers),
            {next_state, ready, State#state{workers=Workers,
                                            monitors=Monitors}};
        false ->
            %% unknown process checked in, ignore it
            {next_state, ready, State}
    end;
ready(_Event, State) ->
    {next_state, ready, State}.

ready({checkout, Block, Timeout}, {FromPid, _}=From, State) ->
    #state{workers = Workers,
           worker_sup = Sup,
           max_overflow = MaxOverflow,
           worker_init = InitFun} = State,
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            Monitors = [{Pid, Ref} | State#state.monitors],
            NextState = case queue:len(Left) of
                0 when MaxOverflow == 0 ->
                    full;
                0 ->
                    overflow;
                _ ->
                    ready
            end,
            {reply, Pid, NextState, State#state{workers=Left,
                                                monitors=Monitors}};
        {empty, Empty} when MaxOverflow > 0 ->
            {Pid, Ref} = new_worker(Sup, FromPid, InitFun),
            Monitors = [{Pid, Ref} | State#state.monitors],
            {reply, Pid, overflow, State#state{workers=Empty,
                                               monitors=Monitors,
                                               overflow=1}};
        {empty, Empty} when Block =:= false ->
            {reply, full, full, State#state{workers=Empty}};
        {empty, Empty} ->
            Waiting = State#state.waiting,
            {next_state, full, State#state{workers=Empty,
                                           waiting=add_waiting(From, Timeout, Waiting)}}
    end;
ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

overflow({checkin, Pid}, #state{overflow=0}=State) ->
    case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref}, Monitors} ->
            erlang:demonitor(Ref),
            NextState = case State#state.size > 0 of
                true -> ready;
                _ -> overflow
            end,
            {next_state, NextState, State#state{overflow=0, monitors=Monitors,
                                                workers=queue:in(Pid, State#state.workers)}};
        false ->
            %% unknown process checked in, ignore it
            {next_state, overflow, State}
    end;
overflow({checkin, Pid}, State) ->
    #state{overflow=Overflow, worker_stop=StopFun} = State,
    case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref}, Monitors} ->
            dismiss_worker(Pid, StopFun),
            erlang:demonitor(Ref),
            {next_state, overflow, State#state{overflow=Overflow-1,
                                               monitors=Monitors}};
        _ ->
            %% unknown process checked in, ignore it
            {next_state, overflow, State}
    end;
overflow(_Event, State) ->
    {next_state, overflow, State}.

overflow({checkout, Block, Timeout}, From, #state{overflow=Overflow,
                                                  max_overflow=MaxOverflow
                                                  }=State) when Overflow >= MaxOverflow ->
    case Block of
        false ->
            {reply, full, full, State};
        Block ->
            Waiting = State#state.waiting,
            {next_state, full, State#state{waiting=add_waiting(From, Timeout, Waiting)}}
    end;
overflow({checkout, _Block, _Timeout}, {From, _}, State) ->
    #state{worker_sup=Sup, overflow=Overflow,
        worker_init=InitFun, max_overflow=MaxOverflow} = State,
    {Pid, Ref} = new_worker(Sup, From, InitFun),
    Monitors = [{Pid, Ref} | State#state.monitors],
    NewOverflow = Overflow + 1,
    Next = case NewOverflow >= MaxOverflow of
        true -> full;
        _ -> overflow
    end,
    {reply, Pid, Next, State#state{monitors=Monitors,
                                   overflow=NewOverflow}};
overflow(_Event, _From, State) ->
    {reply, ok, overflow, State}.

full({checkin, Pid}, State) ->
    #state{waiting = Waiting, max_overflow = MaxOverflow,
           overflow = Overflow, worker_stop = StopFun} = State,
    case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref0}, Monitors} ->
            erlang:demonitor(Ref0),
            case queue:out(Waiting) of
                {{value, {{FromPid, _}=From, Timeout, StartTime}}, Left} ->
                    case wait_valid(StartTime, Timeout) of
                        true ->
                            Ref = erlang:monitor(process, FromPid),
                            Monitors1 = [{Pid, Ref} | Monitors],
                            gen_fsm:reply(From, Pid),
                            {next_state, full, State#state{waiting=Left,
                                                           monitors=Monitors1}};
                        _ ->
                            %% replay this event with cleaned up waiting queue
                            full({checkin, Pid}, State#state{waiting=Left})
                    end;
                {empty, Empty} when MaxOverflow < 1 ->
                    Workers = queue:in(Pid, State#state.workers),
                    {next_state, ready, State#state{workers=Workers, waiting=Empty,
                                                    monitors=Monitors}};
                {empty, Empty} ->
                    dismiss_worker(Pid, StopFun),
                    {next_state, overflow, State#state{waiting=Empty,
                                                       monitors=Monitors,
                                                       overflow=Overflow-1}}
            end;
        false ->
            %% unknown process checked in, ignore it
            {next_state, full, State}
    end;
full(_Event, State) ->
    {next_state, full, State}.

full({checkout, false, _Timeout}, _From, State) ->
    {reply, full, full, State};
full({checkout, _Block, Timeout}, From, #state{waiting=Waiting}=State) ->
    {next_state, full, State#state{waiting=add_waiting(From, Timeout, Waiting)}};
full(_Event, _From, State) ->
    {reply, ok, full, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(get_avail_workers, _From, StateName, State) ->
    Workers = State#state.workers,
    WorkerList = queue:to_list(Workers),
    {reply, WorkerList, StateName, State};
handle_sync_event(get_all_workers, _From, StateName, State) ->
    Sup = State#state.worker_sup,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, StateName, State};
handle_sync_event(get_all_monitors, _From, StateName, State) ->
    Monitors = State#state.monitors,
    {reply, Monitors, StateName, State};
handle_sync_event(stop, _From, _StateName,State) ->
    Sup = State#state.worker_sup,
    exit(Sup, shutdown),
    {stop, normal, ok, State};
handle_sync_event(status, _From, StateName, State) ->
    {reply, {StateName, queue:len(State#state.workers),
            State#state.overflow, length(State#state.monitors)},
        StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, StateName, State}.

handle_info({'DOWN', Ref, _, _, _}, StateName, State) ->
    case lists:keyfind(Ref, 2, State#state.monitors) of
        {Pid, Ref} ->
            exit(Pid, kill),
            {next_state, StateName, State};
        false ->
            {next_state, StateName, State}
    end;
handle_info({'EXIT', Pid, Reason}, StateName, State) ->
    #state{worker_sup = Sup,
           overflow = Overflow,
           waiting = Waiting,
           max_overflow = MaxOverflow,
           worker_init = InitFun} = State,
    case lists:keytake(Pid, 1, State#state.monitors) of
        {value, {_, Ref}, Monitors} -> erlang:demonitor(Ref),
            case StateName of
                ready ->
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {next_state, ready, State#state{workers=queue:in(new_worker(Sup, InitFun), W),
                                                    monitors=Monitors}};
                overflow when Overflow == 0 ->
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {next_state, ready, State#state{workers=queue:in(new_worker(Sup, InitFun), W),
                                                    monitors=Monitors}};
                overflow ->
                    {next_state, overflow, State#state{monitors=Monitors,
                                                       overflow=Overflow-1}};
                full when MaxOverflow < 1 ->
                    case queue:out(Waiting) of
                        {{value, {{FromPid, _}=From, Timeout, StartTime}}, LeftWaiting} ->
                            case wait_valid(StartTime, Timeout) of
                                true ->
                                    MonitorRef = erlang:monitor(process, FromPid),
                                    NewWorker = new_worker(Sup, InitFun),
                                    Monitors2 = [{NewWorker, MonitorRef} | Monitors],
                                    gen_fsm:reply(From, NewWorker),
                                    {next_state, full, State#state{waiting=LeftWaiting,
                                                                   monitors=Monitors2}};
                                _ ->
                                    %% replay it
                                    handle_info({'EXIT', Pid, Reason}, StateName, State#state{waiting=LeftWaiting})
                            end;
                        {empty, Empty} ->
                            Workers2 = queue:in(new_worker(Sup, InitFun), State#state.workers),
                            {next_state, ready, State#state{monitors=Monitors,
                                                            waiting=Empty,
                                                            workers=Workers2}}
                    end;
                full when Overflow =< MaxOverflow ->
                    case queue:out(Waiting) of
                        {{value, {{FromPid, _}=From, Timeout, StartTime}}, LeftWaiting} ->
                            case wait_valid(StartTime, Timeout) of
                                true ->
                                    MonitorRef = erlang:monitor(process, FromPid),
                                    NewWorker = new_worker(Sup, InitFun),
                                    Monitors2 = [{NewWorker, MonitorRef} | Monitors],
                                    gen_fsm:reply(From, NewWorker),
                                    {next_state, full, State#state{waiting=LeftWaiting,
                                                                   monitors=Monitors2}};
                                _ ->
                                    %% replay it
                                    handle_info({'EXIT', Pid, Reason}, StateName, State#state{waiting=LeftWaiting})
                            end;
                        {empty, Empty} ->
                            {next_state, overflow, State#state{monitors=Monitors,
                                                               overflow=Overflow-1,
                                                               waiting=Empty}}
                    end;
                full ->
                    {next_state, full, State#state{monitors=Monitors,
                                                   overflow=Overflow-1}}
            end;
        _ ->
            %% not a monitored pid, is it in the worker queue?
            case queue:member(Pid, State#state.workers) of
                true ->
                    %% a checked in worker died
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {next_state, StateName, State#state{workers=queue:in(new_worker(Sup, InitFun), W)}};
                _ ->
                    %% completely irrelevant pid exited, don't change anything
                    {next_state, StateName, State}
            end
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) -> ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

new_worker(Sup, InitFun) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    {ok, Pid1} = InitFun(Pid),
    link(Pid1),
    Pid1.

new_worker(Sup, FromPid, InitFun) ->
    Pid = new_worker(Sup, InitFun),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Pid, StopFun) ->
    unlink(Pid),
    StopFun(Pid).

prepopulate(0, _Sup, _InitFun) ->
    queue:new();
prepopulate(N, Sup, InitFun) ->
    prepopulate(N, Sup, queue:new(), InitFun).

prepopulate(0, _Sup, Workers, _InitFun) ->
    Workers;
prepopulate(N, Sup, Workers, InitFun) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup, InitFun), Workers), InitFun).

add_waiting(From, Timeout, Queue) ->
    queue:in({From, Timeout, os:timestamp()}, Queue).

wait_valid(infinity, _) ->
    true;
wait_valid(StartTime, Timeout) ->
    Waited = timer:now_diff(os:timestamp(), StartTime),
    (Waited div 1000) < Timeout.
