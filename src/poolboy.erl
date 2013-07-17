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
    waiting :: queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    restart_delay = 0 :: non_neg_integer()
}).

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), Block :: boolean()) -> pid() | full | dead_endpoint.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full | dead_endpoint.
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
    case poolboy:checkout(Pool, true, Timeout) of
        dead_endpoint -> dead_endpoint;
        Worker ->
            try
                Fun(Worker)
            after
                ok = poolboy:checkin(Pool, Worker)
            end
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
init([{restart_delay, Delay} | Rest], WorkerArgs, State) when is_integer(Delay), Delay >= 0 ->
    init(Rest, WorkerArgs, State#state{restart_delay = Delay*1000});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, supervisor = Sup, restart_delay = Delay} = State) ->
    Workers = prepopulate(Size, Sup, Delay),
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

handle_call({checkout, Block, Timeout}, {FromPid, _} = From, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           restart_delay = Delay,
           max_overflow = MaxOverflow} = State,
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, State#state{workers = Left}};
        {empty, Empty} when MaxOverflow > 0, Overflow < MaxOverflow ->
            case new_overflow_worker(Delay, Sup) of
                undefined ->
                    {reply, dead_endpoint, State#state{workers = Empty}};
                Pid ->
                    Ref = monitor(process, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref}),                    
                    {reply, Pid, State#state{workers = Empty, overflow = Overflow + 1}}
            end;
        {empty, Empty} ->
            case ets:info(Monitors, size) of
                0 ->
                    {reply, dead_endpoint, State#state{workers = Empty}};
                _ when Block =:= false ->
                    {reply, full, State#state{workers = Empty}};
                _ ->
                    Waiting = add_waiting(From, Timeout, State#state.waiting),
                    {noreply, State#state{workers = Empty, waiting = Waiting}}
            end
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

handle_info({'DOWN', Ref, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', Ref}) of
        [[Pid]] ->
            Sup = State#state.supervisor,
            ok = supervisor:terminate_child(Sup, Pid),
            %% Don't wait for the EXIT message to come in.
            %% Deal with the worker exit right now to avoid
            %% a race condition with messages waiting in the
            %% mailbox.
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           restart_delay = Delay} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {noreply, State#state{workers = add_new_worker(Delay, Sup, W)}};
                false ->
                    {noreply, State}
            end
    end;

handle_info(delayed_restart, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           restart_delay = Delay} = State,
    case queue:is_empty(State#state.waiting) of
        true ->
            {noreply, State#state{workers = add_new_worker(Delay, Sup, Workers)}};
        false ->
            {noreply, assign_new_worker(State)}
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

%%% Internal functions to deal with optional restarts
-spec add_new_worker(non_neg_integer(), pid(), queue()) -> queue().
add_new_worker(Delay, Sup, Workers) ->
    case extended_new_worker(Delay, Sup) of
        undefined ->
            Workers;
        Pid ->
            queue:in(Pid, Workers)
    end.

-spec new_overflow_worker(non_neg_integer(), pid()) -> pid()|undefined.
new_overflow_worker(0, Sup) ->
    extended_new_worker(0, Sup);
new_overflow_worker(_, Sup) ->
    extended_new_worker(-1, Sup).

-spec extended_new_worker(non_neg_integer()|-1, pid()) -> pid()|undefined.
extended_new_worker(0, Sup) ->
        {ok, Pid} = supervisor:start_child(Sup, []),
        true = link(Pid),
        Pid;
extended_new_worker(Delay, Sup) ->
    try 
        extended_new_worker(0, Sup)
    catch
        error:{badmatch, _} ->
            if Delay > 0 ->
                    erlang:send_after(Delay, self(), delayed_restart),
                    undefined;
               true ->
                    undefined
            end
    end.

-spec assign_new_worker(#state{}) -> #state{}.
assign_new_worker(State) ->
     {{value, {{FromPid, _} = From, Timeout, StartTime}}, LeftWaiting} = 
        queue:out(State#state.waiting),
    case wait_valid(StartTime, Timeout) of
        true ->
            case extended_new_worker(State#state.restart_delay, State#state.supervisor) of
                undefined ->
                    State;
                NewWorker ->
                    MonitorRef = erlang:monitor(process, FromPid),
                    true = ets:insert(State#state.monitors, {NewWorker, MonitorRef}),
                    gen_server:reply(From, NewWorker),
                    State#state{waiting = LeftWaiting}
            end;
        _ ->
            assign_new_worker(State#state{waiting = LeftWaiting})
    end.
%%% End: Internal functions to deal with optional restarts


dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup, _Delay) when N < 1 ->
    queue:new();
prepopulate(N, Sup, Delay) ->
    prepopulate(N, Sup, Delay, queue:new()).

prepopulate(0, _Sup, _Delay, Workers) ->
    Workers;
prepopulate(N, Sup, Delay, Workers) ->
    prepopulate(N-1, Sup, Delay, add_new_worker(Delay, Sup, Workers)).

add_waiting(Pid, Timeout, Queue) ->
    queue:in({Pid, Timeout, os:timestamp()}, Queue).

wait_valid(_StartTime, infinity) ->
    true;
wait_valid(StartTime, Timeout) ->
    Waited = timer:now_diff(os:timestamp(), StartTime),
    (Waited div 1000) < Timeout.

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {{FromPid, _} = From, Timeout, StartTime}}, Left} ->
            case wait_valid(StartTime, Timeout) of
                true ->
                    Ref1 = erlang:monitor(process, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref1}),
                    gen_server:reply(From, Pid),
                    State#state{waiting = Left};
                false ->
                    handle_checkin(Pid, State#state{waiting = Left})
            end;
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           overflow = Overflow,
           restart_delay = Delay} = State,
    case queue:is_empty(State#state.waiting) of
        false ->
            assign_new_worker(State);
        _ when Overflow > 0 ->
            State#state{overflow = Overflow - 1};
        _ ->
            Workers = 
                add_new_worker(Delay, 
                               Sup,
                               queue:filter(fun (P) -> P =/= Pid end, 
                                            State#state.workers)),
            State#state{workers = Workers}
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
