%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, start/1, start/2,
         start_link/1, start_link/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TIMEOUT, 5000).

-record(backoff, {
    sleep   = 0 :: number(),
    mfargs      :: {atom(), atom(), list()}
}).

-record(state, {
    supervisor  :: pid(),
    workers     :: queue(),
    waiting     :: queue(),
    monitors    :: ets:tid(),
    size = 5    :: non_neg_integer(),
    overflow       = 0  :: non_neg_integer(),
    max_overflow   = 10 :: non_neg_integer(),
    backoff     :: undefined | backoff()
}).

-opaque state()   :: #state{}.
-opaque backoff() :: #backoff{}.


%%%===================================================================
%%% API
%%%===================================================================

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    gen_server:call(Pool, {checkout, Block, Timeout}, Timeout).

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

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
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State)
        when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([{backoff, MFA = {M, F, A}} | Rest], WorkerArgs, State)
        when is_atom(M), is_atom(F), is_list(A) ->
    Backoff = #backoff{mfargs = MFA},
    init(Rest, WorkerArgs, State#state{backoff = Backoff});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size} = State) ->
    Workers = prepopulate(Size, State),
    {ok, State#state{workers = Workers}}.

handle_cast({checkin, Worker}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Worker) of
        [{Pid, Ref}] ->
            true = unbind_consumer(Worker, Ref, State),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({checkout, Block, Timeout}, {Consumer, _} = From, State) ->
    #state{workers = Workers,
           overflow = Overflow,
           waiting = Waiting,
           max_overflow = MaxOverflow} = State,
    case queue:out(Workers) of
        {{value, Worker}, Left} ->
            true = bind_consumer(Worker, Consumer, State),
            {reply, Worker, State#state{workers = Left}};
        {empty, _} when MaxOverflow > 0, Overflow < MaxOverflow ->
            case new_worker(State) of
                {ok, Worker} ->
                    true = bind_consumer(Worker, Consumer, State),
                    {reply, Worker, State#state{overflow = Overflow + 1}};
                {wait, _} ->
                    {noreply, State#state{
                        waiting = add_waiting(From, Timeout, Waiting),
                        overflow = Overflow + 1
                    }}
            end;
        {empty, _} when Block == false ->
            {reply, full, State};
        {empty, _} ->
            {noreply, State#state{
                waiting = add_waiting(From, Timeout, Waiting)
            }}
    end;

handle_call({new_worker_spawned, Worker}, {Wrapper, _}, State) ->
    case ets:lookup(State#state.monitors, Wrapper) of
        [{Wrapper, WrapperRef}] ->
            true = unbind_consumer(Wrapper, WrapperRef, State),
            true = link(Worker),

            Backoff = State#state.backoff,
            Backoff#backoff.sleep == 0 orelse error_logger:info_msg(
                "poolboy(~p): worker(~p) started successfuly", [self(), Worker]
            ),

            {reply, ok, handle_checkin(
                Worker, State#state{backoff = Backoff#backoff{sleep = 0}}
            )};
        [] ->
            Reply = {error, monitor_not_found},
            {reply, Reply, State}
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
handle_call(stop, _From, State) ->
    Sup = State#state.supervisor,
    true = exit(Sup, shutdown),
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

handle_info({'DOWN', Ref, _, Pid, _}, State) ->
    #state{monitors = Monitors, supervisor = Sup} = State,
    case ets:match(Monitors, {'$1', Ref}) of
        [[Wrapper]] when Wrapper == Pid ->
            %% future worker is going 'DOWN'
            NewState = handle_worker_exit(
                Wrapper,
                State#state{backoff = inc_sleep(State#state.backoff)}
            ),
            {noreply, NewState};
        [[Worker]] ->
            ok = supervisor:terminate_child(Sup, Worker),
            %% Don't wait for the EXIT message to come in.
            %% Deal with the worker exit right now to avoid
            %% a race condition with messages waiting in the
            %% mailbox.
            NewState = handle_worker_exit(Worker, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;
handle_info({'EXIT', Sup, Reason}, #state{supervisor = Sup}) ->
    %% die in case of supervisor's death
    exit(Reason);
handle_info({'EXIT', Worker, _Reason}, State) ->
    NewState = case ets:lookup(State#state.monitors, Worker) of
        [{Worker, ConsumerRef}] ->
            %% worker was in use
            true = unbind_consumer(Worker, ConsumerRef, State),
            handle_worker_exit(Worker, State);
        [] ->
            case queue:member(Worker, State#state.workers) of
                true ->
                    handle_worker_exit(Worker, State);
                false ->
                    State
            end
    end,
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

-spec new_worker(state()) -> {ok, pid()} | {wait, pid()}.
new_worker(State = #state{backoff = undefined}) ->
    {ok, new_worker_sync(State)};
new_worker(State) ->
    WrapperPid = new_worker_async(State),
    %% we will add future pid's holder as a consumer
    true = bind_consumer(WrapperPid, WrapperPid, State),
    {wait, WrapperPid}.


-spec new_worker_sync(state()) -> pid().
new_worker_sync(#state{supervisor = Sup}) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

-spec new_worker_async(state()) -> pid().
new_worker_async(State = #state{backoff = Backoff}) ->
    Self = self(),
    proc_lib:spawn(fun () ->
        timer:sleep(Backoff#backoff.sleep),
        Pid = new_worker_sync(State),
        ok = gen_server:call(Self, {new_worker_spawned, Pid}),
        unlink(Pid)
    end).

-spec dismiss_worker(Worker :: pid(), state()) -> true.
dismiss_worker(Pid, #state{supervisor = Sup}) ->
    true = unlink(Pid),
    ok = supervisor:terminate_child(Sup, Pid),
    true.

prepopulate(N, _State) when N < 1 ->
    queue:new();
prepopulate(N, State) ->
    prepopulate(N, State, queue:new()).

prepopulate(0, _State, Workers) ->
    Workers;
prepopulate(N, State, Workers) ->
    prepopulate(N-1, State, queue:in(new_worker_sync(State), Workers)).

add_waiting(Pid, Timeout, Queue) ->
    queue:in({Pid, Timeout, os:timestamp()}, Queue).

wait_valid(_StartTime, infinity) ->
    true;
wait_valid(StartTime, Timeout) ->
    Waited = timer:now_diff(os:timestamp(), StartTime),
    (Waited div 1000) < Timeout.

handle_checkin(Pid, State) ->
    #state{waiting = Waiting, overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {{FromPid, _} = From, Timeout, StartTime}}, Left} ->
            case wait_valid(StartTime, Timeout) of
                true ->
                    true = bind_consumer(Pid, FromPid, State),
                    gen_server:reply(From, Pid),
                    State#state{waiting = Left};
                false ->
                    handle_checkin(Pid, State#state{waiting = Left})
            end;
        {empty, _} when Overflow > 0 ->
            true = dismiss_worker(Pid, State),
            State#state{overflow = Overflow - 1};
        {empty, _} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, overflow = 0}
    end.

handle_worker_exit(DeadPid, State) ->
    #state{waiting = Waiting, overflow = Overflow} = State,
    %% XXX: optimize serveral ets:delete calls
    true = ets:delete(State#state.monitors, DeadPid),
    case queue:out(Waiting) of
        {{value, {{FromPid, _} = From, Timeout, StartTime}}, Left} ->
            case wait_valid(StartTime, Timeout) of
                true ->
                    case new_worker(State) of
                        {ok, Worker} ->
                            true = bind_consumer(Worker, FromPid, State),
                            gen_server:reply(From, Worker),
                            State#state{waiting = Left};
                        {wait, _} ->
                            State
                    end;
                false ->
                    handle_worker_exit(DeadPid, State#state{waiting = Left})
            end;
        {empty, _} when Overflow > 0 ->
            State#state{overflow = Overflow - 1};
        {empty, _} ->
            case new_worker(State) of
                {ok, Worker} ->
                    Workers = queue:in(Worker, State#state.workers),
                    State#state{workers = Workers};
                {wait, _} ->
                    State
            end
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

-spec bind_consumer(Worker :: pid(), Consumer :: pid(), state()) -> true.
bind_consumer(Worker, Consumer, #state{monitors = Monitors})
        when is_pid(Worker), is_pid(Consumer) ->
    Ref = erlang:monitor(process, Consumer),
    true = ets:insert(Monitors, {Worker, Ref}).

-spec unbind_consumer(pid(), reference(), state()) -> true.
unbind_consumer(Worker, ConsumerRef, #state{monitors = Monitors})
        when is_pid(Worker), is_reference(ConsumerRef) ->
    true = erlang:demonitor(ConsumerRef),
    true = ets:delete(Monitors, Worker).

-spec inc_sleep(backoff()) -> backoff().
inc_sleep(B = #backoff{sleep = Sleep, mfargs = {M, F, A}}) ->
    B#backoff{sleep = apply(M, F, [Sleep | A])}.
