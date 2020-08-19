%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, child_spec/4, start/1,
         start/2, start_link/1, start_link/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export_type([pool/0]).

-define(TIMEOUT, 5000).
-define(DEFAULT_SIZE, 5).
-define(DEFAULT_TYPE, list).
-define(DEFAULT_STRATEGY, lifo).
-define(DEFAULT_OVERFLOW, 10).

-ifdef(pre17).
-type pid_queue() :: queue().
-else.
-type pid_queue() :: queue:queue().
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
    supervisor :: pid(),
    worker_module :: atom(),
    workers :: pid_queue(),
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size = ?DEFAULT_SIZE :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = ?DEFAULT_OVERFLOW :: non_neg_integer(),
    strategy = ?DEFAULT_STRATEGY :: lifo | fifo
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

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),

    WorkerModule = worker_module(PoolArgs),
    Supervisor =
    case worker_supervisor(PoolArgs) of
        undefined ->
            start_supervisor(WorkerModule, WorkerArgs);
        Sup when is_pid(Sup) ->
            monitor(process, Sup),
            Sup
    end,
    Size = pool_size(PoolArgs),
    Workers = init_workers(Supervisor, WorkerModule, Size),

    MaxOverflow = max_overflow(PoolArgs),
    Overflow = init_overflow(Size, MaxOverflow),

    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    {ok, #state{
            supervisor = Supervisor,
            worker_module = WorkerModule,
            workers = Workers,
            waiting = Waiting,
            monitors = Monitors,
            size = Size,
            overflow = Overflow,
            max_overflow = MaxOverflow,
            strategy = strategy(PoolArgs)
           }}.

start_supervisor(undefined, _WorkerArgs) ->
    error({badarg, "worker_module or worker_supervisor is required"});
start_supervisor(WorkerModule, WorkerArgs) ->
    start_supervisor(WorkerModule, WorkerArgs, 1).

start_supervisor(WorkerModule, WorkerArgs, Retries) ->
    case poolboy_sup:start_link(WorkerModule, WorkerArgs) of
        {ok, NewPid} ->
            NewPid;
        {error, {already_started, Pid}} when Retries > 0 ->
            MRef = erlang:monitor(process, Pid),
            receive {'DOWN', MRef, _, _, _} -> ok
            after ?TIMEOUT -> ok
            end,
            start_supervisor(WorkerModule, WorkerArgs, Retries - 1);
        {error, Error} ->
            exit({no_worker_supervisor, Error})
    end.

init_workers(Sup, Mod, Size) ->
    prepopulate(Size, Sup, Mod).

init_overflow(_Size, _MaxOverflow) ->
    0.

worker_module(PoolArgs) ->
    Is = is_atom(V = proplists:get_value(worker_module, PoolArgs)),
    if not Is -> undefined; true -> V end.

worker_supervisor(PoolArgs) ->
    case find_pid(V = proplists:get_value(worker_supervisor, PoolArgs)) of
        Res = undefined when Res =:= V -> Res;
        Res when is_pid(Res) -> Res;
        Res = undefined when Res =/= V -> exit({noproc, V});
        Res -> exit({Res, V})
    end.

find_pid(undefined) ->
    undefined;
find_pid(Name) when is_atom(Name) ->
    find_pid({local, Name});
find_pid({local, Name}) ->
    whereis(Name);
find_pid({global, Name}) ->
    find_pid({via, global, Name});
find_pid({via, Registry, Name}) ->
    Registry:whereis_name(Name);
find_pid({Name, Node}) ->
    (catch erlang:monitor_node(Node, true)),
    try rpc:call(Node, erlang, whereis, [Name], ?TIMEOUT) of
        {badrpc, Reason} -> Reason;
        Result -> Result
    catch
        _:Reason -> Reason
    end.


pool_size(PoolArgs) ->
    Is = is_integer(V = proplists:get_value(size, PoolArgs)),
    if not Is -> ?DEFAULT_SIZE; true -> V end.

max_overflow(PoolArgs) ->
    Is = is_integer(V = proplists:get_value(max_overflow, PoolArgs)),
    if not Is -> ?DEFAULT_OVERFLOW; true -> V end.

-define(IS_STRATEGY(S), lists:member(S, [lifo, fifo])).
strategy(PoolArgs) ->
    Is = ?IS_STRATEGY(V = proplists:get_value(strategy, PoolArgs)),
    if not Is -> ?DEFAULT_STRATEGY; true -> V end.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
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
           worker_module = Mod,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow,
           strategy = Strategy} = State,
    case get_worker_with_strategy(Workers, Strategy) of
        {{value, Pid},  Left} ->
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{workers = Left}};
        {empty, _Left} when MaxOverflow > 0, Overflow < MaxOverflow ->
            Pid = new_worker(Sup, Mod),
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
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
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    {reply, Workers, State};
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

handle_info({'DOWN', _, process, Pid, Reason}, State = #state{supervisor = Pid}) ->
    {stop, Reason, State};
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
handle_info({'EXIT', Pid, Reason}, State = #state{supervisor = Pid}) ->
    {stop, Reason, State};
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           worker_module = Mod,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = filter_worker_by_pid(Pid, State#state.workers),
                    {noreply, State#state{workers = queue:in(new_worker(Sup, Mod), W)}};
                false ->
                    {noreply, State}
            end
    end;
handle_info({nodedown, Node}, State = #state{supervisor = Sup})
  when Node == erlang:node(Sup) ->
    {stop, nodedown, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State = #state{supervisor = Sup}) ->
    Workers = queue:to_list(State#state.workers),
    ok = lists:foreach(fun (W) -> unlink(W) end, Workers),
    stop_supervisor(Reason, Sup),
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

new_worker(Sup, Mod)  ->
    Node = erlang:node(Sup),
    {ok, Pid} =
    case rpc:pinfo(Sup, registered_name) of
        {registered_name, Name} ->
            case function_exported(Node, Name, start_child, 0) of
                true -> rpc:call(Node, Name, start_child, []);
                false ->
                    Args = child_args(Sup, Mod),
                    supervisor:start_child(Sup, Args)
            end;
        R when R == undefined; R == [] ->
            Args = child_args(Sup, Mod),
            supervisor:start_child(Sup, Args)
    end,
    true = link(Pid),
    Pid.

get_worker_with_strategy(Workers, fifo) ->
    queue:out(Workers);
get_worker_with_strategy(Workers, lifo) ->
    queue:out_r(Workers).

child_args(Sup, Mod) ->
    Node = erlang:node(Sup),
    case supervisor:get_childspec(Sup, Mod) of
        {ok, #{start := {M,F,A}}} ->
            case function_exported(Node, M, F, length(A)) of
                true -> []
            end;
        {ok, {_Id, {M,F,A}, _R, _SD, _T, _M}} ->
            case function_exported(Node, M, F, length(A)) of
                true -> []
            end;
        _ -> []
    end.

function_exported(Node, Module, Name, Arity) ->
    rpc:call(Node, erlang, function_exported, [Module, Name, Arity]).

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

filter_worker_by_pid(Pid, Workers) ->
    queue:filter(fun (WPid) -> WPid =/= Pid end, Workers).

prepopulate(N, _Sup, _Mod) when N < 1 ->
    queue:new();
prepopulate(N, Sup, Mod) ->
    prepopulate(N, Sup, Mod, queue:new()).

prepopulate(0, _Sup, _Mod, Workers) ->
    Workers;
prepopulate(N, Sup, Mod, Workers) ->
    prepopulate(N-1, Sup, Mod, queue:in(new_worker(Sup, Mod), Workers)).

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           worker_module = Mod,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(State#state.waiting) of
        {{value, {From, CRef, MRef}}, LeftWaiting} ->
            NewWorker = new_worker(Sup, Mod),
            true = ets:insert(Monitors, {NewWorker, CRef, MRef}),
            gen_server:reply(From, NewWorker),
            State#state{waiting = LeftWaiting};
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1, waiting = Empty};
        {empty, Empty} ->
            W = filter_worker_by_pid(Pid, State#state.workers),
            Workers = queue:in(new_worker(Sup, Mod), W),
            State#state{workers = Workers, waiting = Empty}
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

stop_supervisor(Reason, Pid) when is_pid(Pid) ->
    case erlang:node(Pid) of
        N when N == node() ->
            exit(Pid, Reason);
        _ when Reason =/= nodedown ->
            catch gen_server:stop(Pid, Reason, ?TIMEOUT);
        _ -> ok
    end.
