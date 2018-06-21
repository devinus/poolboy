%% Poolboy - A hunky Erlang worker pool factory
-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, start/1, start/2,
         start_link/1, start_link/2, stop/1, status/1, full_status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export_type([pool/0]).

-define(TIMEOUT, 5000).

-ifdef(pre17).
-type pid_queue() :: queue().
-else.
-type pid_queue() :: queue:queue().
-endif.

-type pool() ::
    Name :: (atom() | pid()) |
    {Name :: atom(), node()} |
    {local, Name :: atom()} |
    {global, GlobalName :: any()} |
    {via, Module :: atom(), ViaName :: any()}.

% Copied from gen:start_ret/0
-type start_ret() :: {'ok', pid()} | 'ignore' | {'error', term()}.

-record(state, {
    worker_supervisor :: pid(),
    workers :: queue:queue(),
    total_workers_started = 0 :: non_neg_integer(),
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo,
    overflow_ttl = 0 :: non_neg_integer(),
    reap_timer = none,
    checkin_callback = fun({_Pid, _Reason}) -> keep end :: function()
}).

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{worker_supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([{strategy, lifo} | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State#state{strategy = lifo});
init([{strategy, fifo} | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State#state{strategy = fifo});
init([{overflow_ttl, OverflowTtl} | Rest], WorkerArgs, State) when is_integer(OverflowTtl) ->
    init(Rest, WorkerArgs, State#state{overflow_ttl = OverflowTtl});
init([{checkin_callback, CheckinCallback} | Rest], WorkerArgs, State) when is_function(CheckinCallback, 1) ->
    init(Rest, WorkerArgs, State#state{checkin_callback = CheckinCallback});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, worker_supervisor = Sup, total_workers_started = Counter} = State) ->
    Workers = prepopulate(Size, Sup, Counter),
    {ok, State#state{workers = Workers, total_workers_started = Size}}.

-spec checkout(Pool :: pool()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: pool(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: pool(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    CRef = make_ref(),
    try
        gen_server:call(Pool, {checkout, CRef, Block}, Timeout)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            gen_server:cast(Pool, {cancel_waiting, CRef}),
            erlang:raise(Class, Reason, Stack)
    end.

-spec checkin(Pool :: pool(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, Worker}).

-spec transaction(Pool :: pool(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).

-spec transaction(Pool :: pool(), Fun :: fun((Worker :: pid()) -> any()),
    Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

-spec child_spec(PoolId :: term(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs) ->
    child_spec(PoolId, PoolArgs, []).

-spec child_spec(PoolId :: term(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs) ->
    {PoolId, {poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [poolboy]}.

-spec start(PoolArgs :: proplists:proplist())
    -> start_ret().
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> start_ret().
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> start_ret().
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: pool()) -> ok.
stop(Pool) ->
    gen_server:call(Pool, stop).

-spec status(Pool :: pool()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_server:call(Pool, status).

-spec full_status(Pool :: pool()) -> proplists:proplist().
full_status(Pool) ->
    gen_server:call(Pool, full_status).

handle_cast({checkin, Pid}, State = #state{monitors = Monitors, checkin_callback = Callback}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            KillOrKeep = Callback({Pid, normal}),
            NewState = handle_checkin(Pid, State, KillOrKeep),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, CRef}, State = #state{monitors = Monitors, checkin_callback = Callback}) ->
    case ets:match(Monitors, {'$1', CRef, '$2'}) of
        [[Pid, MRef]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.monitors, Pid),
            KillOrKeep = Callback({Pid, normal}),
            NewState = handle_checkin(Pid, State, KillOrKeep),
            {noreply, NewState};
        [] ->
            Cancel = fun({_, Ref, MRef}) when Ref =:= CRef ->
                             demonitor(MRef, [flush]),
                             false;
                        (_) ->
                             true
                     end,
            Waiting = queue:filter(Cancel, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({checkout, CRef, Block}, {FromPid, _} = From, State) ->
    #state{worker_supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow,
           strategy = Strategy,
           total_workers_started = WorkersStarted} = State,
    case get_worker_with_strategy(Workers, Strategy) of
        {{value, {_Time, Pid}}, Left} when State#state.overflow_ttl > 0 ->
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            NewState = State#state{workers = Left},
            {ok, ReapTimer} = reset_worker_reap(NewState, Pid),
            {reply, Pid, NewState#state{reap_timer = ReapTimer}};
        {{value, {_Time, Pid}}, Left} ->
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{workers = Left}};
        {empty, _Left} when MaxOverflow > 0, Overflow < MaxOverflow ->
            {Pid, MRef} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{overflow = Overflow + 1, total_workers_started = WorkersStarted + 1}};
        {empty, _Left} when Block =:= false ->
            {reply, full, State};
        {empty, _Left} ->
            MRef = erlang:monitor(process, FromPid),
            Waiting = queue:in({From, CRef, MRef}, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow} = State,
    CheckedOutWorkers = ets:info(Monitors, size),
    StateName = state_name(State),
    {reply, {StateName, queue:len(Workers), Overflow, CheckedOutWorkers}, State};
handle_call(full_status, _From, State) ->
    #state{workers = Workers,
        size = Size,
        monitors = Monitors,
        overflow = Overflow,
        max_overflow = MaxOverflow,
        worker_supervisor = Sup,
        waiting = Waiting,
        total_workers_started = WorkersStarted} = State,
    CheckedOutWorkers = ets:info(Monitors, size),
    {reply,
        [
            {size, Size}, % The permanent worker size
            {max_overflow, MaxOverflow}, % The overflow size
            % The maximum amount of worker is size + overflow_size

            {total_worker_count, length(supervisor:which_children(Sup))}, % The total of all workers
            {ready_worker_count, queue:len(Workers)}, % Number of workers ready to use
            {overflow_worker_count, Overflow}, % Number of overflow workers
            {checked_out_worker_count, CheckedOutWorkers}, % Number of workers currently checked out
            {waiting_request_count, queue:len(Waiting)}, % Number of waiting requests
            {total_workers_started, WorkersStarted} % Number of workers started, helps establish how bad churn is
        ],
        State
    };
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    {reply, Workers, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.worker_supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};
handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:select(State#state.monitors,
                          [{{'$1', '_', '$2'}, [], [{{'$1', '$2'}}]}]),
    {reply, Monitors, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

handle_info({'DOWN', MRef, _, _, _}, State = #state{monitors = Monitors, checkin_callback = Callback}) ->
    case ets:match(Monitors, {'$1', '_', MRef}) of
        [[Pid]] ->
            true = ets:delete(State#state.monitors, Pid),
            KillOrKeep = Callback({Pid, owner_death}),
            NewState = handle_checkin(Pid, State, KillOrKeep),
            {noreply, NewState};
        [] ->
            Waiting = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checked_out_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            NewState = handle_checked_in_worker_exit(State, Pid),
            {noreply, NewState}
    end;
handle_info({reap_worker, Pid}, State)->
    NewState = reap_worker(Pid, State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Workers = queue:to_list(State#state.workers),
    ok = lists:foreach(fun ({_Time, Pid}) -> unlink(Pid) end, Workers),
    true = exit(State#state.worker_supervisor, shutdown),
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

handle_checkin(Pid, State, keep) ->
    #state{worker_supervisor = Sup,
        waiting = Waiting,
        monitors = Monitors,
        overflow = Overflow,
        overflow_ttl = OverflowTtl,
        reap_timer = ReapTimer} = State,
    case queue:out(Waiting) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        {empty, Empty} when Overflow > 0, OverflowTtl > 0 ->
            CurrentTime = erlang:monotonic_time(milli_seconds),
            {ok, Timer} = set_reap_timer(ReapTimer, CurrentTime, OverflowTtl, Pid),
            Workers = queue:in({CurrentTime, Pid}, State#state.workers),
            State#state{workers = Workers, waiting = Empty, reap_timer = Timer};
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in({erlang:monotonic_time(milli_seconds), Pid}, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end;
handle_checkin(Pid, #state{waiting = {[],[]}, overflow = Overflow} = State, kill) when Overflow > 0 ->
    W = remove_worker(Pid, State),
    State#state{workers=W, overflow = Overflow - 1};
handle_checkin(Pid, State, kill) ->
    W = remove_worker(Pid, State),
    NewWorker = new_worker(State#state.worker_supervisor),
    handle_checkin(NewWorker, State#state{workers = W}, keep).

handle_checked_out_worker_exit(Pid, State) ->
    #state{worker_supervisor = Sup,
        monitors = Monitors,
        overflow = Overflow,
        total_workers_started = WorkersStarted} = State,
    case queue:out(State#state.waiting) of
        {{value, {From, CRef, MRef}}, LeftWaiting} ->
            NewWorker = new_worker(State#state.worker_supervisor),
            true = ets:insert(Monitors, {NewWorker, CRef, MRef}),
            gen_server:reply(From, NewWorker),
            State#state{waiting = LeftWaiting, total_workers_started = WorkersStarted + 1};
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1, waiting = Empty};
        {empty, Empty} ->
            W = delete_worker_by_pid(Pid, State#state.workers),
            Workers = queue:in({erlang:monotonic_time(milli_seconds), new_worker(Sup)}, W),
            State#state{workers = Workers, waiting = Empty, total_workers_started = WorkersStarted + 1}
    end.

handle_checked_in_worker_exit(State = #state{overflow = Overflow}, Pid) when Overflow > 0 ->
    #state{workers = Workers} = State,
    NewWorkers = delete_worker_by_pid(Pid, Workers),
    {ok, ReapTimer} = reset_worker_reap(State#state{workers = NewWorkers}, Pid),
    State#state{workers = NewWorkers, reap_timer = ReapTimer, overflow = Overflow - 1};
handle_checked_in_worker_exit(State, Pid) ->
    #state{workers = Workers, total_workers_started = WorkersStarted, worker_supervisor = Sup} = State,
    % Have to remove the existing worker before resetting timer
    FilteredWorkers = delete_worker_by_pid(Pid, Workers),
    NewWorkers = queue:in({erlang:monotonic_time(milli_seconds), new_worker(Sup)}, FilteredWorkers),
    {ok, ReapTimer} = reset_worker_reap(State#state{workers = NewWorkers}, Pid),
    State#state{workers = NewWorkers, reap_timer = ReapTimer, total_workers_started = WorkersStarted + 1}.

prepopulate(N, _Sup, _Counter) when N < 1 ->
    queue:new();
prepopulate(N, Sup, Counter) ->
    prepopulate(N, Sup, queue:new(), Counter).

prepopulate(0, _Sup, Workers, _Counter) ->
    Workers;
prepopulate(N, Sup, Workers, Counter) ->
    prepopulate(N-1, Sup, queue:in({erlang:monotonic_time(milli_seconds), new_worker(Sup)}, Workers), Counter + 1).

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

remove_worker(Pid, State) ->
    #state{workers = Workers,
        worker_supervisor = Sup} = State,
    W = delete_worker_by_pid(Pid, Workers),
    ok = dismiss_worker(Sup, Pid),
    W.

reap_worker(Pid, #state{overflow = Overflow} = State) when Overflow == 1 ->
    W = remove_worker(Pid, State),
    State#state{workers = W, overflow = Overflow -1, reap_timer = none};
reap_worker(Pid, State = #state{overflow = Overflow}) when Overflow > 1 ->
    #state{reap_timer = ReapTimer,
        overflow_ttl = OverflowTtl} = State,
    W = remove_worker(Pid, State),
    {ok, Timer} = reset_reap_timer(W, ReapTimer, OverflowTtl),
    State#state{workers = W, overflow = Overflow -1, reap_timer = Timer};
reap_worker(_Pid, State) ->
    State.


reset_worker_reap(#state{reap_timer = none} = _State, Pid) ->
    ok = clear_worker_reap_message(Pid),
    {ok, none};
reset_worker_reap(#state{overflow = Overflow, reap_timer = ReapTimer} = _State, Pid) when Overflow == 0 ->
    erlang:cancel_timer(ReapTimer),
    ok = clear_worker_reap_message(Pid),
    {ok, ReapTimer};
reset_worker_reap(#state{reap_timer = ReapTimer, overflow = Overflow} = State, Pid) when Overflow > 0 ->
    #state{workers = Workers,
        overflow_ttl = OverflowTtl} = State,
    erlang:cancel_timer(ReapTimer),
    {ok, NewTimer} = reset_reap_timer(Workers, ReapTimer, OverflowTtl),
    ok = clear_worker_reap_message(Pid),
    {ok, NewTimer}.

set_reap_timer(ReapTimer, _CurrentTime, _OverflowTtl, _Pid) when is_reference(ReapTimer) ->
    {ok, ReapTimer};
set_reap_timer(_ReapTimer, CurrentTime, OverflowTtl, Pid) ->
    NewTimer = erlang:send_after(CurrentTime + OverflowTtl, self(), {reap_worker, Pid}, [{abs, true}]),
    {ok, NewTimer}.

reset_reap_timer(Workers, ReapTimer, OverflowTtl)->
    erlang:cancel_timer(ReapTimer),
    case queue:peek(Workers) of
        {value, {Time, OldestWorker}} ->
            ReapTime = Time + OverflowTtl,
            NewTimer = erlang:send_after(ReapTime, self(), {reap_worker, OldestWorker}, [{abs, true}]),
            {ok, NewTimer};
        empty ->
            {ok, none}
    end.

clear_worker_reap_message(Pid) ->
    receive
        {reap_worker, Pid} ->
            ok
    after 0 ->
        ok
    end.

delete_worker_by_pid(Pid, Workers) ->
    queue:filter(fun ({_Time, WPid}) -> WPid =/= Pid end, Workers).

get_worker_with_strategy(Workers, fifo) ->
    queue:out(Workers);
get_worker_with_strategy(Workers, lifo) ->
    queue:out_r(Workers).

state_name(State = #state{overflow = Overflow, max_overflow = MaxOverflow}) when Overflow < 1->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case queue:len(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(State = #state{overflow = Overflow, max_overflow = MaxOverflow}) when Overflow == MaxOverflow->
    #state{workers = Workers} = State,
    case queue:len(Workers) of
        NumberOfWorkers when NumberOfWorkers > 0 -> ready;
        _NumberOfWorkers -> full
    end;
state_name(State = #state{overflow = Overflow, max_overflow = MaxOverflow}) when Overflow < MaxOverflow->
    #state{workers = Workers} = State,
    case queue:len(Workers) of
        NumberOfWorkers when NumberOfWorkers > 0 -> ready;
        _NumberOfWorkers -> overflow
    end.

