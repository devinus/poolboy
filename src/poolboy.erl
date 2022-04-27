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

-ifdef(pre17).
%-type pid_queue() :: queue().
-else.
-type pid_queue() :: queue:queue().
-endif.

%% this implies 21 or higher
-ifdef(OTP_RELEASE). 
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
    strategy = lifo :: lifo | fifo
}).

%% @private
-type state() :: #state{
	supervisor :: undefined | pid(),
    workers :: undefined | pid_queue(),
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size  :: non_neg_integer(),
    overflow :: non_neg_integer(),
    max_overflow :: non_neg_integer(),
    strategy :: lifo | fifo
	}.

%% @doc Removes pool item from the pool.
%% @returns pid of a worker.
%% @equiv checkout(Pool, true)

-spec checkout(Pool) -> Result when
	Pool :: pool(),
	Result :: pid().
checkout(Pool) ->
    checkout(Pool, true).

%% @doc Removes pool item from the pool. 
%% Default wait time for a reply is set to `5000'.
%% @returns pid of a worker or `full' atom.
%% @equiv checkout(Pool, Block, TIMEOUT)

-spec checkout(Pool, Block) -> Result when
	Pool :: pool(), 
	Block :: boolean(),
	Result :: pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

%% @doc Removes pool item from the pool.
%% @param Timeout is an integer greater than zero that 
%% specifies how many milliseconds to wait for a reply, 
%% or the atom `infinity' to wait indefinitely. 
%% @returns pid of a worker or `full' atom.

-spec checkout(Pool, Block, Timeout) -> Result when
	Pool :: pool(), 
	Block :: boolean(), 
	Timeout :: timeout(),
	Result :: pid() | full.
checkout(Pool, Block, Timeout) ->
    CRef = make_ref(),
    try
        gen_server:call(Pool, {checkout, CRef, Block}, Timeout)
    catch
        ?EXCEPTION(Class, Reason, Stacktrace) ->
            gen_server:cast(Pool, {cancel_waiting, CRef}),
            erlang:raise(Class, Reason, ?GET_STACK(Stacktrace))
    end.

%% @doc Asynchronous try to add worker `pid' to the pool.
%% @equiv gen_server:cast(Pool, {checkin, Worker})

-spec checkin(Pool, Worker) -> Result when
	Pool :: pool(), 
	Worker :: pid(),
	Result :: ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, Worker}).

%% @doc Run function `Fun' using a worker from the pool.
%% @equiv transaction(Pool, Fun, TIMEOUT)

-spec transaction(Pool, Fun) -> Result when
	Pool :: pool(), 
	Fun :: fun((Worker) -> any()),
	Worker :: pid(),
	Result :: any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).

%% @doc Run function `Fun' using a worker from the pool. 
%% `Timeout' is a time in millisecods while the pool get 
%% a worker from it.
%% @param Timeout is an integer greater than zero that 
%% specifies how many milliseconds to wait getting 
%% a worker from the pool. 

-spec transaction(Pool, Fun, Timeout) -> Result when
	Pool :: pool(),
	Fun :: fun((Worker :: pid()) -> any()),
	Timeout :: timeout(),
	Result :: any().
transaction(Pool, Fun, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

%% @doc Create child specification of a supervisor.
%% @equiv child_spec(PoolId, PoolArgs, [])

-spec child_spec(PoolId, PoolArgs) -> Result when
	PoolId :: term(), 
	PoolArgs :: proplists:proplist(),
	Result :: supervisor:child_spec().
child_spec(PoolId, PoolArgs) ->
    child_spec(PoolId, PoolArgs, []).

%% @doc Create child specification of a supervisor.
%% @equiv child_spec(PoolId, PoolArgs, WorkerArgs, tuple)

-spec child_spec(PoolId, PoolArgs, WorkerArgs) -> Result when
	PoolId :: term(),
    PoolArgs :: proplists:proplist(),
    WorkerArgs :: proplists:proplist(),
	Result :: supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs) ->
    child_spec(PoolId, PoolArgs, WorkerArgs, tuple).

%% @doc Create child specification of a supervisor.

-spec child_spec(PoolId, PoolArgs, WorkerArgs, ChildSpecFormat) -> Result when
	PoolId :: term(),
    PoolArgs :: proplists:proplist(),
    WorkerArgs :: proplists:proplist(),
    ChildSpecFormat :: 'tuple' | 'map',
	Result :: supervisor:child_spec().
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

%% @doc Creates a standalone `gen_server' process, 
%% that is, a `gen_server' process that is not part of 
%% a supervision tree and thus has no supervisor.
%% @equiv start(PoolArgs, PoolArgs)

-spec start(PoolArgs) -> Result when
	PoolArgs :: proplists:proplist(),
	Result :: start_ret().
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

%% @doc Creates a standalone `gen_server' process, 
%% that is, a `gen_server' process that is not part of 
%% a supervision tree and thus has no supervisor.

-spec start(PoolArgs, WorkerArgs) -> Result when
	PoolArgs :: proplists:proplist(),
    WorkerArgs:: proplists:proplist(),
	Result :: start_ret().
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

%% @doc Creates a `gen_server' process as part of 
%% a supervision tree. This function is to be called, 
%% directly or indirectly, by the supervisor. 
%% For example, it ensures that the gen_server process 
%% is linked to the supervisor.
%% @equiv start_link(PoolArgs, PoolArgs)

-spec start_link(PoolArgs) -> Result when
	PoolArgs :: proplists:proplist(),
	Result :: start_ret().
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

%% @doc Creates a `gen_server' process as part of 
%% a supervision tree. This function is to be called, 
%% directly or indirectly, by the supervisor. 
%% For example, it ensures that the gen_server process 
%% is linked to the supervisor.

-spec start_link(PoolArgs, WorkerArgs) -> Result when
	PoolArgs :: proplists:proplist(),
    WorkerArgs:: proplists:proplist(),
	Result :: start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

%% @doc Stop pool of workers.
%% @equiv gen_server:call(Pool, stop)

-spec stop(Pool) -> Result when
	Pool :: pool(),
	Result :: ok.
stop(Pool) ->
    gen_server:call(Pool, stop).

%% @doc Get pool description.
%% @returns Result :: {StateName, QueueLength, Overflow, Size}
%% <br/>
%% <code>StateName: </code> status of the pool<br/>
%% <code>QueueLength: </code> length of worker queue<br/>
%% <code>Overflow: </code> maximum number of workers created if pool is empty<br/>
%% <code>Size: </code> The number of objects inserted in the table<br/>
%% @equiv gen_server:call(Pool, status)

-spec status(Pool) -> Result when
	Pool :: pool(),
	Result :: {StateName, QueueLength, Overflow, Size},
	StateName :: full | overflow | ready,
	QueueLength :: non_neg_integer(),
	Overflow :: non_neg_integer(),
	Size :: non_neg_integer().
	
status(Pool) ->
    gen_server:call(Pool, status).

%% @private
%% @equiv init(PoolArgs, WorkerArgs, state())

-spec init(Args) -> Result when
	Args :: {PoolArgs, WorkerArgs},
	PoolArgs :: proplists:proplist(),
    WorkerArgs:: proplists:proplist(),
	Result :: {ok, state()}.
init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

-spec init(List, WorkerArgs, State) -> Result when
	List :: [{worker_module, Mod} | Rest],
	Mod :: module(),
	Rest :: list(),
	WorkerArgs:: proplists:proplist(),
	State :: state(),
	Result :: {ok, state()};
(List, WorkerArgs, State) -> Result when
	List :: [{size, Size} | Rest],
	Size :: non_neg_integer(),
	Rest :: list(),
	WorkerArgs:: proplists:proplist(),
	State :: state(),
	Result :: {ok, state()};
(List, WorkerArgs, State) -> Result when
	List :: [{max_overflow, MaxOverflow} | Rest],
	MaxOverflow :: non_neg_integer(),
	Rest :: list(),
	WorkerArgs:: proplists:proplist(),
	State :: state(),
	Result :: {ok, state()};
(List, WorkerArgs, State) -> Result when
	List :: [{strategy, lifo} | Rest],
	Rest :: list(),
	WorkerArgs:: proplists:proplist(),
	State :: state(),
	Result :: {ok, state()};
(List, WorkerArgs, State) -> Result when
	List :: [{strategy, fifo} | Rest],
	Rest :: list(),
	WorkerArgs:: proplists:proplist(),
	State :: state(),
	Result :: {ok, state()};
(List, WorkerArgs, State) -> Result when
	List :: list(),
	WorkerArgs:: proplists:proplist(),
	State :: state(),
	Result :: {ok, state()}.
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
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, supervisor = Sup} = State) ->
    Workers = prepopulate(Size, Sup),
    {ok, State#state{workers = Workers}}.

%% @private

-spec handle_cast(Request, State) -> Result when 
	Request :: {checkin, Pid},
	Pid :: pid(),
	State :: state(),
	Result :: {noreply, State};
(Request, State) -> Result when 
	Request :: {cancel_waiting, CRef},
	CRef :: reference(),
	State :: state(),
	Result :: {noreply, State};
(Request, State) -> Result when
	Request	:: term(),
	State :: state(),
	Result :: {noreply, State}.
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

%% @private

-spec handle_call(Request, From, State) -> Result when
	Request :: {checkout, CRef, Block},
	CRef :: reference(),
	Block :: boolean(), 
	From :: {pid(), atom()},
    State :: state(),
	Result :: {reply, Pid, state()} | {reply, full, state()} | {noreply, state()},
	Pid :: pid();
(Request, From, State) -> Result when
	Request :: status,
	From :: {pid(), atom()},
    State :: state(),
	Result :: {reply, {StateName, QueueLength, Overflow, Value}, State},
	StateName :: full | overflow | ready,
	QueueLength :: non_neg_integer(),
	Overflow :: non_neg_integer(),
	Value :: term();
(Request, From, State) ->	Result when
	Request :: get_avail_workers,
	From :: {pid(), atom()},
    State :: state(),
	Result :: {reply, Workers, State},
	Workers :: undefined | pid_queue();
(Request, From, State) -> Result when
	Request :: get_all_workers,
	From :: {pid(), atom()},
    State :: state(),
	Result :: {reply, WorkerList, State},
	WorkerList :: [{Id, Child, Type, Modules}],
	Id :: supervisor:child_id() | undefined,
	Child :: supervisor:child() | restarting,
	Type :: supervisor:worker(),
	Modules :: supervisor:modules();
(Request, From, State) -> Result when
	Request :: get_all_monitors,
	From :: {pid(), atom()},
    State :: state(),
	Result :: {reply, Monitors, State},
	Monitors :: ets:tid();
(Request, From, State) -> Result when
	Request :: stop,
	From :: {pid(), atom()},
    State :: state(),
	Result :: {stop, normal, ok, State}.
handle_call({checkout, CRef, Block}, {FromPid, _} = From, State) ->
    #state{supervisor = Sup,
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
            {Pid, MRef} = new_worker(Sup, FromPid),
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

%% @private

-spec handle_info(Info, State) -> Result when
	Info :: {'DOWN', MonitorRef, any(), any(), any()},
	MonitorRef :: reference(),
	State :: state(),
	Result :: {noreply, state()};
(Info, State) -> Result when
	Info :: {'EXIT', Pid, Reason},
	Pid :: pid(),
	Reason :: term(),
	State :: state(),
	Result :: {noreply, state()};
(Info, State) -> Result when
	Info :: timeout | term(),
	State :: state(),
	Result :: {noreply, state()}.
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
                    {noreply, State#state{workers = queue:in(new_worker(Sup), W)}};
                false ->
                    {noreply, State}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private

-spec terminate(Reason, State) -> Result when
	Reason :: normal | shutdown | {shutdown,term()} | term(),
	State :: state(),
	Result :: ok.
terminate(_Reason, State) ->
    Workers = queue:to_list(State#state.workers),
    ok = lists:foreach(fun (W) -> unlink(W) end, Workers),
    true = exit(State#state.supervisor, shutdown),
    ok.

%% @private
- spec code_change(OldVsn, State, Extra) -> Result when
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term(),
	State :: state(),
	Extra :: term(),
	Result :: {ok, NewState} | {error, Reason},
	NewState :: term(),
	Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec start_pool(StartFun, PoolArgs, WorkerArgs) -> Result when
	StartFun :: start | start_link,
	PoolArgs :: proplists:proplist(),
    WorkerArgs:: proplists:proplist(),
	Result :: start_ret().
start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

-spec new_worker(Supervisor) -> Result when
	Supervisor :: pid(),
	Result :: pid().
new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

-spec new_worker(Supervisor, FromPid) -> Result when
	Supervisor :: pid(),
	FromPid :: pid(),
	Result :: pid().
new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

-spec get_worker_with_strategy(Workers, Strategy) -> Result when
	Workers :: pid_queue(), 
	Strategy :: lifo | fifo,
	Result :: {Value,  Left} | {Value, Left},
	Value :: empty | {value, pid()},
	Left :: pid_queue().
get_worker_with_strategy(Workers, fifo) ->
    queue:out(Workers);
get_worker_with_strategy(Workers, lifo) ->
    queue:out_r(Workers).

-spec dismiss_worker(Supervisor, Pid) -> Result when
	Supervisor :: pid(),
	Pid :: pid(),
	Result ::  ok | {error, Error},
	Error :: not_found | simple_one_for_one.
dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

-spec filter_worker_by_pid(Pid, Workers) -> Result when
	Pid :: pid(), 
	Workers :: pid_queue(), 
	Result :: pid_queue().
filter_worker_by_pid(Pid, Workers) ->
    queue:filter(fun (WPid) -> WPid =/= Pid end, Workers).

-spec prepopulate(Count, Supervisor) -> Result when
	Count :: integer(), 
	Supervisor :: pid(),
	Result :: pid_queue().
prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

-spec prepopulate(Count, Supervisor, Workers) -> Result when
	Count :: integer(), 
	Supervisor :: pid(),
	Workers :: pid_queue(), 
	Result :: pid_queue().
prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).

-spec handle_checkin(Pid, State) -> Result when
	Pid :: pid(), 
	State :: state(),
	Result :: state().
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

-spec handle_worker_exit(Pid, State) -> Result when
	Pid :: pid(), 
	State :: state(),
	Result :: state().
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
            W = filter_worker_by_pid(Pid, State#state.workers),
            Workers = queue:in(new_worker(Sup), W),
            State#state{workers = Workers, waiting = Empty}
    end.

-spec state_name(State) -> Result when
	State :: state(),
	Result :: full | overflow | ready.
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
