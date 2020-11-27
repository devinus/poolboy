%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, child_spec/4, start/1,
         start/2, start_link/1, start_link/2, stop/1, status/1, stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export_type([pool/0]).

-define(TIMEOUT, 5000).

-ifdef(pre17).
-type pid_queue() :: queue().
-else.
-type pid_queue() :: queue:queue().
-endif.

-ifdef(pre18).
-define(timestamp(), timestamp()).
timestamp() ->
    {M, S, Mi} = os:timestamp(),
    (M*1000000 + S)*1000000 + Mi.
-else.
-define(timestamp(), erlang:monotonic_time()).
-endif.

-ifdef(OTP_RELEASE). %% this implies 21 or higher
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-else.
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-define(GET_STACK(_), erlang:get_stacktrace()).
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
    supervisor :: undefined | pid(),
    workers :: undefined | pid_queue(),
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo,
    stats = false,
    %% quant_start is set in init/3 function
    %% because of timestamping issues on erlang/otp 17
    quant_start = undefined,
    time_used = 0
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
        ?EXCEPTION(Class, Reason, Stacktrace) ->
            gen_server:cast(Pool, {cancel_waiting, CRef}),
            erlang:raise(Class, Reason, ?GET_STACK(Stacktrace))
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
    child_spec(PoolId, PoolArgs, WorkerArgs, tuple).

-spec child_spec(PoolId :: term(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist(),
                 ChildSpecFormat :: 'tuple' | 'map')
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs, tuple) ->
    {PoolId, {poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [poolboy]};
child_spec(PoolId, PoolArgs, WorkerArgs, map) ->
    #{id => PoolId,
      start => {poolboy, start_link, [PoolArgs, WorkerArgs]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [poolboy]}.

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

-spec stats(Pool :: pool()) -> {ok, float()} | {error, disabled}.
stats(Pool) ->
    case gen_server:call(Pool, stats) of
        {error, disabled} -> {error, disabled};
        {ok, {TimeTotal, TimeUsed, Workers}} ->
            {ok, (TimeUsed / TimeTotal / Workers)}
    end.


init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
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
init([{stats, StatsEnabled} | Rest], WorkerArgs, State) when is_boolean(StatsEnabled) ->
    init(Rest, WorkerArgs, State#state{stats = StatsEnabled});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, supervisor = Sup} = State) ->
    Workers = prepopulate(Size, Sup),
    {ok, State#state{workers = Workers, quant_start = ?timestamp()}}.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef, MaybeStartedAt}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, MaybeStartedAt, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, CRef}, State) ->
    case ets:match(State#state.monitors, {'$1', CRef, '$2', '$3'}) of
        [[Pid, MRef, MaybeStartedAt]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, MaybeStartedAt, State),
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
           max_overflow = MaxOverflow,
           strategy = Strategy,
           stats = StatsEnabled} = State,
    case get_worker_with_strategy(Workers, Strategy) of
        {{value, Pid},  Left} ->
            MRef = erlang:monitor(process, FromPid),
            MaybeStartedAt = if StatsEnabled -> ?timestamp();
                                true -> undefined end,
            true = ets:insert(Monitors, {Pid, CRef, MRef, MaybeStartedAt}),
            {reply, Pid, State#state{workers = Left}};
        {empty, _Left} when MaxOverflow > 0, Overflow < MaxOverflow ->
            {Pid, MRef} = new_worker(Sup, FromPid),
            MaybeStartedAt = if StatsEnabled -> ?timestamp();
                                true -> undefined end,
            true = ets:insert(Monitors, {Pid, CRef, MRef, MaybeStartedAt}),
            {reply, Pid, State#state{overflow = Overflow + 1}};
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
    StateName = state_name(State),
    {reply, {StateName, queue:len(Workers), Overflow, ets:info(Monitors, size)}, State};
handle_call(stats, _From, #state{stats = false} = State) ->
    {reply, {error, disabled}, State};
handle_call(stats, _From, State) ->
    {reply, {ok, {
        ?timestamp() - State#state.quant_start,
        State#state.time_used, State#state.size
    }}, State#state{quant_start = ?timestamp(), time_used = 0}};
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    {reply, Workers, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};
handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:select(State#state.monitors,
                          [{{'$1', '_', '$2', '_'}, [], [{{'$1', '$2'}}]}]),
    {reply, Monitors, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

handle_info({'DOWN', MRef, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', '_', MRef, '$2'}) of
        [[Pid, MaybeStartedAt]] ->
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, MaybeStartedAt, State),
            {noreply, NewState};
        [] ->
            Waiting = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef, MaybeStartedAt}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, MaybeStartedAt, State),
            {noreply, NewState};
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = filter_worker_by_pid(Pid, State#state.workers),
                    {noreply, State#state{workers = queue:in(new_worker(Sup), W)}};
                false ->
                    {noreply, State}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Workers = queue:to_list(State#state.workers),
    ok = lists:foreach(fun (W) -> unlink(W) end, Workers),
    true = exit(State#state.supervisor, shutdown),
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

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

get_worker_with_strategy(Workers, fifo) ->
    queue:out(Workers);
get_worker_with_strategy(Workers, lifo) ->
    queue:out_r(Workers).

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

filter_worker_by_pid(Pid, Workers) ->
    queue:filter(fun (WPid) -> WPid =/= Pid end, Workers).

prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).

handle_checkin(Pid, StartedAt, State0) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow,
           stats = StatsEnabled} = State0,
    State1 = handle_checkin_stats(StatsEnabled, StartedAt, State0),
    case queue:out(Waiting) of
        {{value, {From, CRef, MRef}}, Left} ->
            MaybeStartedAt = if StatsEnabled -> ?timestamp();
                                true -> undefined end,
            true = ets:insert(Monitors, {Pid, CRef, MRef, MaybeStartedAt}),
            gen_server:reply(From, Pid),
            State1#state{waiting = Left};
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State1#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in(Pid, State1#state.workers),
            State1#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_checkin_stats(false, _, State) -> State;
handle_checkin_stats(_, undefined, State) -> State;
handle_checkin_stats(_, StartedAt, #state{time_used = TimeUsed} = State) ->
    State#state{time_used = TimeUsed + (?timestamp() - StartedAt)}.

handle_worker_exit(Pid, StartedAt, State0) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           overflow = Overflow,
           stats = StatsEnabled} = State0,
    State1 = handle_checkin_stats(StatsEnabled, StartedAt, State0),
    case queue:out(State1#state.waiting) of
        {{value, {From, CRef, MRef}}, LeftWaiting} ->
            NewWorker = new_worker(State1#state.supervisor),
            MaybeStartedAt = if StatsEnabled -> ?timestamp();
                                true -> undefined end,
            true = ets:insert(Monitors, {NewWorker, CRef, MRef, MaybeStartedAt}),
            gen_server:reply(From, NewWorker),
            State1#state{waiting = LeftWaiting};
        {empty, Empty} when Overflow > 0 ->
            State1#state{overflow = Overflow - 1, waiting = Empty};
        {empty, Empty} ->
            W = filter_worker_by_pid(Pid, State1#state.workers),
            Workers = queue:in(new_worker(Sup), W),
            State1#state{workers = Workers, waiting = Empty}
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
