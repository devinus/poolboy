%% Poolboy - A hunky Erlang worker pool factory
-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, start/1, start/2,
         start_link/1, start_link/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export_type([pool/0]).

-define(TIMEOUT, 5000).
-define(CHECK_OVERFLOW_DEFAULT_PERIOD, 1000000).  %microseconds (1 sec)

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
    supervisor :: pid(),
    workers :: [pid()],
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo,
    overflow_check_period :: non_neg_integer(), %milliseconds
    overflow_ttl = 0 :: non_neg_integer()   %microseconds
}).

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
            gen_server:cast(Pool, {cancel_waiting, CRef}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
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

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([{strategy, lifo} | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State#state{strategy = lifo});
init([{strategy, fifo} | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State#state{strategy = fifo});
init([{overflow_ttl, OverflowTtl} | Rest], WorkerArgs, State) when is_integer(OverflowTtl) ->
    init(Rest, WorkerArgs, State#state{overflow_ttl = OverflowTtl * 1000});
init([{overflow_check_period, OverflowCheckPeriod} | Rest], WorkerArgs, State) when is_integer(OverflowCheckPeriod) ->
    init(Rest, WorkerArgs, State#state{overflow_check_period = OverflowCheckPeriod});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, supervisor = Sup} = State) ->
    Workers = prepopulate(Size, Sup),
    start_timer(State),
    {ok, State#state{workers = Workers}}.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:take(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, CRef}, State) ->
    case ets:match(State#state.monitors, {'$1', CRef, '$2'}) of
        [[Pid, MRef]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
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
    #state{supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    case Workers of
        [{Pid, _} | Left] when State#state.overflow_ttl > 0 ->
            add_worker_execute(Pid, CRef, FromPid, Monitors),
            {reply, Pid, State#state{workers = Left}};
        [{Pid, _} | Left] ->
            add_worker_execute(Pid, CRef, FromPid, Monitors),
            {reply, Pid, State#state{workers = Left}};
        [] when MaxOverflow > 0, Overflow < MaxOverflow ->
            Pid = new_worker(Sup),
            add_worker_execute(Pid, CRef, FromPid, Monitors),
            {reply, Pid, State#state{overflow = Overflow + 1}};
        [] when Block =:= false ->
            {reply, full, State};
        [] ->
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
    {reply, {StateName, length(Workers), Overflow, CheckedOutWorkers}, State};
handle_call(get_avail_workers, _From, State = #state{workers = Workers}) ->
    Pids = lists:map(fun({Pid, _TS}) -> Pid end, Workers),
    {reply, Pids, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
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

handle_info({'DOWN', MRef, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', '_', MRef}) of
        [[Pid]] ->
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            Waiting = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           workers = Workers} = State,
    case ets:take(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            case lists:keymember(Pid, 1, Workers) of
                true ->
                    W = lists:keydelete(Pid, 1, Workers),
                    {noreply, State#state{workers = [{new_worker(Sup), erlang:monotonic_time(micro_seconds)} | W]}};
                false ->
                    {noreply, State}
            end
    end;
handle_info({rip_workers, TimerTTL}, State) ->
    #state{overflow = Overflow,
        overflow_ttl = OverTTL,
        workers = Workers,
        strategy = Strategy,
        supervisor = Sup} = State,
    Now = erlang:monotonic_time(micro_seconds),
    {UOverflow, UWorkers} = do_reap_workers(Workers, Now, OverTTL, Overflow, Sup, [], Strategy),
    erlang:send_after(TimerTTL, self(), {rip_workers, TimerTTL}),
    {noreply, State#state{overflow = UOverflow, workers = UWorkers}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{workers = Workers, supervisor = Sup}) ->
    ok = lists:foreach(fun ({W, _}) -> unlink(W) end, Workers),
    true = exit(Sup, shutdown),
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

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup) when N < 1 ->
    [];
prepopulate(N, Sup) ->
    prepopulate(N, Sup, []).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, [{new_worker(Sup), erlang:monotonic_time(micro_seconds)} | Workers]).

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow,
           strategy = Strategy,
           overflow_ttl = OverflowTtl} = State,
    case queue:out(Waiting) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        {empty, Empty} when Overflow > 0, OverflowTtl > 0 ->
            LastUseTime = erlang:monotonic_time(micro_seconds),
            Workers = return_worker(Strategy, {Pid, LastUseTime}, State#state.workers),
            State#state{workers = Workers, waiting = Empty};
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            LastUseTime = erlang:monotonic_time(micro_seconds),
            Workers = return_worker(Strategy, {Pid, LastUseTime}, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(State#state.waiting) of
        {{value, {From, CRef, MRef}}, LeftWaiting} ->
            NewWorker = new_worker(State#state.supervisor),
            true = ets:insert(Monitors, {NewWorker, CRef, MRef}),
            gen_server:reply(From, NewWorker),
            State#state{waiting = LeftWaiting};
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1, waiting = Empty};
        {empty, Empty} ->
            Workers =
                [{new_worker(Sup), erlang:monotonic_time(micro_seconds)}
                 | lists:filter(fun (P) -> P =/= Pid end, State#state.workers)],
            State#state{workers = Workers, waiting = Empty}
    end.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case length(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(State = #state{overflow = Overflow}) when Overflow > 0 ->
    #state{max_overflow = MaxOverflow, workers = Workers, overflow = Overflow} = State,
    NumberOfWorkers = length(Workers),
    case MaxOverflow == Overflow of
        true when NumberOfWorkers > 0 -> ready;
        true -> full;
        false -> overflow
    end;
state_name(_State) ->
    overflow.

add_worker_execute(WorkerPid, CRef, FromPid, Monitors) ->
    MRef = erlang:monitor(process, FromPid),
    true = ets:insert(Monitors, {WorkerPid, CRef, MRef}).

return_worker(lifo, Pid, Workers) -> [Pid | Workers];
return_worker(fifo, Pid, Workers) -> Workers ++ [Pid].

do_reap_workers(UnScanned, _, _, 0, _, Workers, _) ->
    {0, lists:reverse(Workers) ++ UnScanned};
do_reap_workers([], _, _, Overflow, _, Workers, _) ->
    {Overflow, lists:reverse(Workers)};
do_reap_workers([Worker = {Pid, LTime} | Rest], Now, TTL, Overflow, Sup, Workers, Strategy) ->
    case Now - LTime > TTL of
        true ->
            ok = dismiss_worker(Sup, Pid),
            do_reap_workers(Rest, Now, TTL, Overflow - 1, Sup, Workers, Strategy);
        false when Strategy =:= lifo ->
            {Overflow, lists:reverse([Worker | Workers]) ++ Rest};
        false ->
            do_reap_workers(Rest, Now, TTL, Overflow, Sup, [Worker | Workers], Strategy)
    end.

start_timer(#state{overflow_ttl = 0}) -> %overflow turned off
    ok;
start_timer(#state{overflow_check_period = TimerTTL}) when is_number(TimerTTL) ->
    erlang:send_after(TimerTTL, self(), {rip_workers, TimerTTL});
start_timer(#state{overflow_check_period = undefined, overflow_ttl = TTL}) ->
    TimerTTL = round(min(TTL, ?CHECK_OVERFLOW_DEFAULT_PERIOD) / 1000), %If overflow_ttl is less, than a second - period will be same as overflow_ttl
    erlang:send_after(TimerTTL, self(), {rip_workers, TimerTTL}).
