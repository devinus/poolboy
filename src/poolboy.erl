%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, child_spec/4, start/1,
         start/2, start_link/1, start_link/2, stop/1, status_map/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export_type([pool/0]).

-define(TIMEOUT, 5000).
-define(DEFAULT_SIZE, 5).
-define(DEFAULT_TYPE, list).
-define(IS_COLLECTION_TYPE(Type), (Type == list orelse
                                   Type == array orelse
                                   Type == tuple orelse
                                   Type == queue)).



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

% Copied from supervisor:sup_ref/0
-type sup_ref() ::
    (Name :: atom()) |
    {Name :: atom(), Node :: node()} |
    {global, Name :: atom()} |
    {via, Module :: module(), Name :: any()} |
    pid().

-record(state, {
    supervisor :: sup_ref(),
    worker_module :: atom(),
    workers :: poolboy_collection:coll() | poolboy_collection:coll(pid()),
    waiting :: poolboy_collection:pid_queue() | poolboy_collection:pid_queue(tuple()),
    monitors :: ets:tid(),
    size = ?DEFAULT_SIZE :: non_neg_integer(),
    overflow :: poolboy_collection:coll() | poolboy_collection:coll(pid()),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo
}).

-type status_key() ::
      state | available | overflow | monitored | waiting.

-type state_name() ::
      full | overflow | ready.

-type status_map() ::
      #{status_key() := integer() | state_name()}.

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

-spec status_map(Pool :: pool()) -> status_map().
status_map(Pool) ->
    gen_server:call(Pool, status_map).

-spec status(Pool :: pool()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_server:call(Pool, status).

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    WorkerModule = proplists:get_value(worker_module, PoolArgs),
    Supervisor = ensure_worker_supervisor(PoolArgs, WorkerArgs),
    Workers = prepopulate(PoolArgs, Supervisor, WorkerModule),
    Overflow = prepopulate(0, ?DEFAULT_TYPE, Supervisor, WorkerModule),
    init(PoolArgs, WorkerArgs,
         #state{supervisor = Supervisor,
                worker_module = WorkerModule,
                workers = Workers,
                waiting = Waiting,
                monitors = Monitors,
                overflow = Overflow
               }).

ensure_worker_supervisor(PoolArgs, WorkerArgs) ->
    case proplists:get_value(worker_supervisor, PoolArgs) of
        undefined ->
            start_supervisor(
              proplists:get_value(worker_module, PoolArgs),
              WorkerArgs);
        Sup = {Name, Node} when Name =/= local orelse
                                Name =/= global ->
            (catch erlang:monitor_node(Node, true)),
            Sup;
        Sup when is_pid(Sup) orelse
                 is_atom(Sup) orelse
                 is_tuple(Sup) ->
            Sup
    end.

start_supervisor(WorkerModule, WorkerArgs) ->
    start_supervisor(WorkerModule, WorkerArgs, 1).

start_supervisor(undefined, _WorkerArgs, _Retries) ->
    exit({no_worker_supervisor, {worker_module, undefined}});
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

prepopulate(PoolArgs, Supervisor, WorkerModule) ->
    prepopulate(
      proplists:get_value(size, PoolArgs, ?DEFAULT_SIZE),
      proplists:get_value(type, PoolArgs, ?DEFAULT_TYPE),
      Supervisor,
      WorkerModule).

prepopulate(Size, Type, Sup, Mod)
  when is_integer(Size) andalso ?IS_COLLECTION_TYPE(Type) ->
    poolboy_collection:new(Type, Size, fun(Idx) -> new_worker(Sup, Mod, Idx) end);
prepopulate(Size, Type, Sup, Mod) when not is_integer(Size) ->
    prepopulate(?DEFAULT_SIZE, Type, Sup, Mod);
prepopulate(Size, Type, Sup, Mod) when not ?IS_COLLECTION_TYPE(Type) ->
    prepopulate(Size, ?DEFAULT_TYPE, Sup, Mod).

init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State = #state{size = Size})
  when is_integer(MaxOverflow) ->
    Overflow = poolboy_collection:new(?DEFAULT_TYPE, MaxOverflow,
                                      fun(Idx) -> Size + Idx end),
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow,
                                       overflow = Overflow});
init([{strategy, Strategy} | Rest], WorkerArgs, State)
  when Strategy == lifo orelse
       Strategy == fifo ->
    init(Rest, WorkerArgs, State#state{strategy = Strategy});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, State) ->
    {ok, State}.

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
           max_overflow = MaxOverflow} = State,
    OverflowLeft = poolboy_collection:length(visible, Overflow),
    case poolboy_collection:hide_head(Workers) of
        {Pid, Left} when is_pid(Pid) ->
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{workers = Left}};
        empty when MaxOverflow > 0, OverflowLeft > 0 ->
            MRef = erlang:monitor(process, FromPid),
            {NextIdx, NewOverflow} = poolboy_collection:hide_head(Overflow),
            Pid = new_worker(Sup, Mod, NextIdx),
            {Pid, NewerOverflow} = poolboy_collection:replace(NextIdx, Pid, NewOverflow),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{overflow = NewerOverflow}};
        empty when Block =:= false ->
            {reply, full, State};
        empty ->
            MRef = erlang:monitor(process, FromPid),
            Waiting = queue:in({From, CRef, MRef}, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_call(status_map, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    StateName = state_name(State),
    OverflowLeft = poolboy_collection:length(visible, Overflow),
    OverflowLevel = MaxOverflow - OverflowLeft,
    {reply, #{state => StateName,
              available => poolboy_collection:length(visible, Workers),
              overflow => OverflowLevel,
              monitored => ets:info(Monitors, size),
              waiting => queue:len(State#state.waiting)}, State};
handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    StateName = state_name(State),
    VisibleWorkers = poolboy_collection:length(visible, Workers),
    OverflowLeft = poolboy_collection:length(visible, Overflow),
    OverflowLevel = MaxOverflow - OverflowLeft,
    MonitorSize = ets:info(Monitors, size),
    {reply, {StateName, VisibleWorkers, OverflowLevel, MonitorSize}, State};
handle_call(get_avail_workers, _From, State) ->
    {reply, poolboy_collection:all(visible, State#state.workers), State};
handle_call(get_any_worker, _From, State) ->
    {reply, poolboy_collection:rand(known, State#state.workers), State};
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
handle_info({'EXIT', Pid, Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    Next =
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            try
                {noreply, replace_worker(Pid, State)}
            catch ?EXCEPTION(error, enoent, _StackTrace) ->
                {noreply, State}
            end
    end,
    case {Sup, erlang:node(Pid)} of
        {{_, Node}, Node} -> {stop, Reason, State#state{supervisor = undefined}};
        {Pid, _} -> {stop, Reason, State#state{supervisor = undefined}};
        _ -> Next
    end;
handle_info({nodedown, Node}, State = #state{supervisor = {_, Node}}) ->
    {stop, nodedown, State#state{supervisor = undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State = #state{supervisor = Sup}) ->
    poolboy_collection:filter(fun (W) -> catch not unlink(W) end, State#state.workers),
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

new_worker(Sup, Mod, Index) when is_atom(Sup) ->
    IsStart1 = erlang:function_exported(Sup, start_child, 1),
    IsStart0 = erlang:function_exported(Sup, start_child, 0),
    {ok, Pid} =
    case {IsStart1, IsStart0} of
        {true, _} -> Sup:start_child(Index);
        {_, true} -> Sup:start_child();
        _ -> supervisor:start_child(Sup, child_args(Sup, Mod, Index))
    end,
    true = link(Pid),
    Pid;
new_worker(Sup, Mod, Index) ->
    {ok, Pid} = supervisor:start_child(Sup, child_args(Sup, Mod, Index)),
    true = link(Pid),
    Pid.

child_args(Sup, Mod, Index) ->
    case supervisor:get_childspec(Sup, Mod) of
        {ok, #{start := {M,F,A}}} ->
            case erlang:function_exported(M, F, length(A) + 1) of
                true -> [Index];
                _ -> []
            end;
        _ -> []
    end.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

replace_worker(Pid, State = #state{strategy = Strategy}) ->
    {NewWorker, Workers} = poolboy_collection:replace(Pid, State#state.workers),
    case Strategy of
        lifo -> State#state{workers = poolboy_collection:prepend(NewWorker, Workers)};
        fifo -> State#state{workers = poolboy_collection:append(NewWorker, Workers)}
    end.

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           size = Size,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        {empty, Empty} ->
            try poolboy_collection:replace(Pid, fun(Idx) -> Size + Idx end, Overflow) of
                {NewIdx, NewOverflow} ->
                    ok = dismiss_worker(Sup, Pid),
                    NewerOverflow = poolboy_collection:prepend(NewIdx, NewOverflow),
                    State#state{waiting = Empty, overflow = NewerOverflow}
            catch
                error:enoent ->
                    Workers =
                    case State#state.strategy of
                        lifo -> poolboy_collection:prepend(Pid, State#state.workers);
                        fifo -> poolboy_collection:append(Pid, State#state.workers)
                    end,
                    State#state{waiting = Empty, workers = Workers}
            end
    end.

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           worker_module = Mod,
           monitors = Monitors,
           size = Size,
           strategy = Strategy,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    {NewWorker, Workers} =
    try poolboy_collection:replace(Pid, State#state.workers)
    catch error:enoent -> {enoent, State#state.workers}
    end,
    OverflowLeft = poolboy_collection:length(visible, Overflow),
    case queue:out(State#state.waiting) of
        {{value, {From, CRef, MRef}}, LeftWaiting} when is_pid(NewWorker) ->
            true = ets:insert(Monitors, {NewWorker, CRef, MRef}),
            gen_server:reply(From, NewWorker),
            State#state{waiting = LeftWaiting, workers = Workers};
        {{value, {From, CRef, MRef}}, LeftWaiting} when MaxOverflow > OverflowLeft ->
            try
                NewFun = fun(Idx) -> new_worker(Sup, Mod, Size + Idx) end,
                {NewPid, NewOverflow} = poolboy_collection:replace(Pid, NewFun, Overflow),
                true = ets:insert(Monitors, {NewPid, CRef, MRef}),
                gen_server:reply(From, NewPid),
                State#state{waiting = LeftWaiting, overflow = NewOverflow}
            catch error:enoent ->
                State
            end;
        {empty, Empty} when is_pid(NewWorker), Strategy == lifo ->
            State#state{waiting = Empty,
                        workers = poolboy_collection:prepend(NewWorker, Workers)};
        {empty, Empty} when is_pid(NewWorker), Strategy == fifo ->
            State#state{waiting = Empty,
                        workers = poolboy_collection:append(NewWorker, Workers)};
        {empty, Empty} when MaxOverflow > 0 ->
            {Idx, NewOverflow} = poolboy_collection:replace(Pid, Overflow),
            NewerOverflow = poolboy_collection:prepend(Idx, NewOverflow),
            State#state{waiting = Empty, overflow = NewerOverflow}
    end.

state_name(State) ->
    #state{workers = Workers,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    case poolboy_collection:length(visible, Workers) of
        0 when MaxOverflow < 1 -> full;
        0 ->
            case poolboy_collection:length(visible, Overflow) of
                0 -> full;
                _ -> overflow
            end;
        _ -> ready
    end.

stop_supervisor(_, undefined) -> ok;
stop_supervisor(Reason, Atom) when is_atom(Atom) ->
    stop_supervisor(Reason, whereis(Atom));
stop_supervisor(Reason, Pid) when is_pid(Pid) ->
    case erlang:node(Pid) of
        N when N == node() -> exit(Pid, Reason);
        _ when Reason =/= nodedown -> catch gen_server:stop(Pid, Reason, ?TIMEOUT);
        _ -> ok
    end;
stop_supervisor(nodedown, Tuple) when is_tuple(Tuple) -> ok;
stop_supervisor(Reason, Tuple) when is_tuple(Tuple) ->
    catch gen_server:stop(Tuple, Reason, ?TIMEOUT).
