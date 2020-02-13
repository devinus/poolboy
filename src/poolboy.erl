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
    workers :: poolboy_collection:coll() | poolboy_collection:coll(pid()),
    waiting :: poolboy_collection:pid_queue() | poolboy_collection:pid_queue(tuple()),
    monitors :: ets:tid(),
    size = ?DEFAULT_SIZE :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo
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
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    Supervisor = ensure_worker_supervisor(PoolArgs, WorkerArgs),
    Workers = prepopulate(PoolArgs, Supervisor),
    init(PoolArgs, WorkerArgs,
         #state{supervisor = Supervisor,
                workers = Workers,
                waiting = Waiting,
                monitors = Monitors}).

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

prepopulate(PoolArgs, Supervisor) ->
    prepopulate(
      proplists:get_value(size, PoolArgs, ?DEFAULT_SIZE),
      proplists:get_value(type, PoolArgs, ?DEFAULT_TYPE),
      Supervisor).

prepopulate(Size, Type, Sup)
  when is_integer(Size) andalso ?IS_COLLECTION_TYPE(Type) ->
    poolboy_collection:new(Type, Size, fun(_) -> new_worker(Sup) end);
prepopulate(Size, Type, Sup) when not is_integer(Size) ->
    prepopulate(?DEFAULT_SIZE, Type, Sup);
prepopulate(Size, Type, Sup) when not ?IS_COLLECTION_TYPE(Type) ->
    prepopulate(Size, ?DEFAULT_TYPE, Sup).

init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State)
  when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
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
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    case poolboy_collection:hide_head(Workers) of
        {Pid, Left} when is_pid(Pid) ->
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{workers = Left}};
        empty when MaxOverflow > 0, Overflow < MaxOverflow ->
            {Pid, MRef} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{overflow = Overflow + 1}};
        empty when Block =:= false ->
            {reply, full, State};
        empty ->
            MRef = erlang:monitor(process, FromPid),
            Waiting = queue:in({From, CRef, MRef}, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow} = State,
    StateName = state_name(State),
    {reply, {StateName, poolboy_collection:length(visible, Workers), Overflow, ets:info(Monitors, size)}, State};
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

new_worker(Sup) ->
    {ok, Pid} =
    case is_atom(Sup) andalso erlang:function_exported(Sup, start_child, 0) of
        true -> Sup:start_child();
        false -> supervisor:start_child(Sup, [])
    end,
    true = link(Pid),
    Pid.

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

replace_worker(Pid, State = #state{strategy = Strategy}) ->
    {NewWorker, Workers} = poolboy_collection:replace(Pid, State#state.workers),
    case Strategy of
        lifo -> State#state{workers = poolboy_collection:prepend(NewWorker, Workers)};
        fifo -> State#state{workers = poolboy_collection:append(NewWorker, Workers)}
    end.

handle_checkin(Pid, State = #state{strategy = Strategy}) ->
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
            Workers = case Strategy of
                lifo -> poolboy_collection:prepend(Pid, State#state.workers);
                fifo -> poolboy_collection:append(Pid, State#state.workers)
            end,
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_worker_exit(Pid, State) ->
    #state{monitors = Monitors,
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
            replace_worker(Pid, State#state{waiting = Empty})
    end.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case poolboy_collection:length(visible, Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(#state{overflow = MaxOverflow, max_overflow = MaxOverflow}) ->
    full;
state_name(_State) ->
    overflow.

stop_supervisor(_, undefined) -> ok;
stop_supervisor(Reason, Pid) when is_pid(Pid) ->
    case erlang:node(Pid) of
        N when N == node() -> exit(Pid, Reason);
        _ when Reason =/= nodedown -> catch gen_server:stop(Pid, Reason, ?TIMEOUT);
        _ -> ok
    end;
stop_supervisor(nodedown, Tuple) when is_tuple(Tuple) -> ok;
stop_supervisor(Reason, Tuple) when is_tuple(Tuple) ->
    catch gen_server:stop(Tuple, Reason, ?TIMEOUT).
