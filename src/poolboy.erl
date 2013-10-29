%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, start/1, start/2,
         start_link/1, start_link/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TIMEOUT, 5000).

-record(state, {
    supervisor :: pid(),
    workers :: queue(),
    workers_monitors :: ets:tid(), %% refs to workers
    waiting :: queue(),
    monitors :: ets:tid(), %% refs to processes which acquired workers
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer()
}).

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, infinity) ->
    gen_server:call(Pool, {checkout, Block, infinity}, infinity);
checkout(Pool, Block, Timeout) ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    Deadline = {MegaSecs, Secs, MicroSecs + Timeout},
    gen_server:call(Pool, {checkout, Block, Deadline}, Timeout).

-spec checkin(Pool :: node(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, Worker}).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()), 
    Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

-spec child_spec(Pool :: node(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs) ->
    child_spec(Pool, PoolArgs, []).

-spec child_spec(Pool :: node(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs, WorkerArgs) ->
    {Pool, {poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [poolboy]}.

-spec start(PoolArgs :: proplists:proplist())
    -> {ok, pid()}.
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> {ok, pid()}.
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> {ok, pid()}.
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> {ok, pid()}.
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: node()) -> ok.
stop(Pool) ->
    gen_server:call(Pool, stop).

-spec status(Pool :: node()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_server:call(Pool, status).

init({PoolArgs, WorkerArgs}) ->
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    WorkersMonitors = ets:new(workers_monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting,
                                      workers_monitors = WorkersMonitors,
                                      monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, State = #state{workers_monitors = WorkersMonitors,
                                     size = Size,
                                     supervisor = Sup}) ->
    Workers = prepopulate(Size, Sup, WorkersMonitors),
    {ok, State#state{workers = Workers}}.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({checkout, Block, Deadline}, {FromPid, _} = From, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           workers_monitors = WorkersMonitors,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, State#state{workers = Left}};
        {empty, Empty} when MaxOverflow > 0, Overflow < MaxOverflow ->
            {Pid, Ref} = new_worker(Sup, FromPid, WorkersMonitors),
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, State#state{workers = Empty, overflow = Overflow + 1}};
        {empty, Empty} when Block =:= false ->
            {reply, full, State#state{workers = Empty}};
        {empty, Empty} ->
            Waiting = add_waiting(From, Deadline, State#state.waiting),
            {noreply, State#state{workers = Empty, waiting = Waiting}}
    end;

handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow} = State,
    StateName = state_name(State),
    {reply, {StateName, queue:len(Workers), Overflow, ets:info(Monitors, size)}, State};
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    WorkerList = queue:to_list(Workers),
    {reply, WorkerList, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};
handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:tab2list(State#state.monitors),
    {reply, Monitors, State};
handle_call(get_all_workers_monitors, _From, State) ->
    WorkersMonitors = ets:tab2list(State#state.workers_monitors),
    {reply, WorkersMonitors, State};
handle_call(stop, _From, State) ->
    Sup = State#state.supervisor,
    true = exit(Sup, shutdown),
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

handle_info({'DOWN', Ref, _, _, _}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           workers_monitors = WorkersMonitors} = State,
    case ets:match(Monitors, {'$1', Ref}) of
        [[Pid]] -> %% was it an owner? Then shutdown the worker.
            ok = supervisor:terminate_child(Sup, Pid),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            case ets:match(WorkersMonitors, {'$1', Ref}) of
                [[Pid]] -> %% was it a worker? Then demonitor the owner and get
                           %% new worker from supervisor.
                    case ets:lookup(Monitors, Pid) of
                        [{Pid, AquirerRef}] ->
                            true = ets:delete(Monitors, Pid),
                            erlang:demonitor(AquirerRef);
                        [] ->
                            ok
                    end,
                    NewState = handle_worker_exit(Pid, State),
                    {noreply, NewState};
                _ ->
                    {noreply, State}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

new_worker(Sup, WorkersMonitors) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    Ref = erlang:monitor(process, Pid),
    ets:insert(WorkersMonitors, {Pid, Ref}),
    Pid.

new_worker(Sup, FromPid, WorkersMonitors) ->
    Pid = new_worker(Sup, WorkersMonitors),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Sup, Pid, WorkersMonitors) ->
    [{Pid, Ref}] = ets:lookup(WorkersMonitors, Pid),
    erlang:demonitor(Ref),
    true = ets:delete(WorkersMonitors, Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup, _WorkersMonitors) when N < 1 ->
    queue:new();
prepopulate(N, Sup, WorkersMonitors) ->
    prepopulate(N, Sup, queue:new(), WorkersMonitors).

prepopulate(0, _Sup, Workers, _WorkersMonitors) ->
    Workers;
prepopulate(N, Sup, Workers, WorkersMonitors) ->
    prepopulate(N-1, Sup, queue:in(
                            new_worker(Sup, WorkersMonitors), Workers),
                WorkersMonitors).

add_waiting(Pid, Deadline, Queue) ->
    queue:in({Pid, Deadline}, Queue).

past_deadline(infinity) ->
    false;
past_deadline(Deadline) ->
    timer:now_diff(os:timestamp(), Deadline) < 0.

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           workers_monitors = WorkersMonitors,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {{FromPid, _} = From, Deadline}}, Left} ->
            case past_deadline(Deadline) of
                false ->
                    Ref1 = erlang:monitor(process, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref1}),
                    gen_server:reply(From, Pid),
                    State#state{waiting = Left};
                true ->
                    handle_checkin(Pid, State#state{waiting = Left})
            end;
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid, WorkersMonitors),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           workers_monitors = WorkersMonitors,
           monitors = Monitors,
           size = Size,
           overflow = Overflow} = State,
    true = ets:delete(WorkersMonitors, Pid),
    case queue:out(State#state.waiting) of
        {{value, {{FromPid, _} = From, Deadline}}, LeftWaiting} ->
            case past_deadline(Deadline) of
                false ->
                    MonitorRef = erlang:monitor(process, FromPid),
                    NewWorker = new_worker(Sup, WorkersMonitors),
                    true = ets:insert(Monitors, {NewWorker, MonitorRef}),
                    gen_server:reply(From, NewWorker),
                    State#state{waiting = LeftWaiting};
                true ->
                    handle_worker_exit(Pid, State#state{waiting = LeftWaiting})
            end;
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1,
                        waiting = Empty,
                        workers = Workers};
        {empty, Empty} ->
            FilteredWorkers = queue:filter(fun (P) -> P =/= Pid end, Workers),
            NewWorkers = case FilteredWorkers of
                             Workers ->
                                 case queue:len(Workers) < Size of
                                     true ->
                                         queue:in(new_worker(Sup, WorkersMonitors),
                                                  Workers);
                                     false ->
                                         Workers
                                 end;
                             _ ->
                                 queue:in(new_worker(Sup, WorkersMonitors),
                                          FilteredWorkers)
                         end,
            State#state{workers = NewWorkers, waiting = Empty}
    end.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case queue:len(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(#state{overflow = MaxOverflow, max_overflow = MaxOverflow}) ->
    full;
state_name(_State) ->
    overflow.
