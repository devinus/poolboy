%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% a re-write of poolboy_eqc in the grouped style.
%%% WhY? There is a counter example (unknown_ce.eqc) that passes when
%%% eqc:check/2 is called with poolboy_eqc:prop_parallel/0. In order
%%% to get a deterministic falilure we need a PULSE test, and before
%%% writing that it makes sense to re-write this test in the modern
%%% grouped style.
%%% @end
%%% Created : 28 Sep 2017 by Russell Brown <russell@wombat.me>

-module(poolboy_statem).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

eqc_test_() ->
    {timeout, 20,
        fun() ->
                ?assert(eqc:quickcheck(eqc:testing_time(4,
                            poolboy_statem:prop_sequential()))),
                ?assert(eqc:quickcheck(eqc:testing_time(4,
                            poolboy_statem:prop_parallel())))
        end
    }.

%% -- State ------------------------------------------------------------------
-record(state,
        {
          pid,
          size,
          max_overflow,
          checked_out = [],
          workers = []
        }).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Commons ----------------------------------------------------------------
%% If pid is not set there is no poolboy, and the only Cmd possible is start
command_precondition_common(S, Cmd) ->
    S#state.pid /= undefined orelse Cmd == start_poolboy.

%% -- Operations -------------------------------------------------------------

%% --- Operation: start_poolboy ---
%% @doc start_poolboy_pre/1 - Precondition for generation
-spec start_poolboy_pre(S :: eqc_statem:symbolic_state()) -> boolean().
start_poolboy_pre(S) ->
   S#state.pid == undefined.

%% @doc start_poolboy_args - Argument generator
-spec start_poolboy_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
start_poolboy_args(_S) ->
    ?LET({Size, Overflow},
         {{size, nat()}, {max_overflow, nat()}},
         [[Size, Overflow,
           {worker_module, poolboy_test_worker},
           {name, {local, ?MODULE}}]]).

%% @doc start_poolboy - The actual operation
start_poolboy(Args) ->
    {ok, Pid} = poolboy:start_link(Args),
    Pid.

%% @doc start_poolboy_next - Next state function
-spec start_poolboy_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
start_poolboy_next(S, Pool, [Args]) ->
    Size = proplists:get_value(size, Args),
    S#state{pid = Pool,
            size = Size,
            workers = [ {{worker, N}, undefined} || N <- lists:seq(1, Size) ],
            max_overflow = proplists:get_value(max_overflow, Args)
           }.

%% --- Operation: stop_poolboy ---
%% @doc stop_poolboy_args - Argument generator
-spec stop_poolboy_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
stop_poolboy_args(#state{pid=Pid}) ->
    [Pid].

%% @doc stop_poolboy - The actual operation
stop_poolboy(Pool) ->
    gen_fsm:sync_send_all_state_event(Pool, stop),
    timer:sleep(1).

%% @doc stop_poolboy_next - Next state function
-spec stop_poolboy_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
stop_poolboy_next(S, _Value, [_Pool]) ->
    S#state{pid=undefined, checked_out=[], workers=[]}.

%% --- Operation: checkout_nonblock ---
%% @doc checkout_nonblock_args - Argument generator
-spec checkout_nonblock_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
checkout_nonblock_args(#state{pid=Pool}) ->
    [Pool].

%% @doc checkout_nonblock - The actual operation
checkout_nonblock(Pool) ->
    proxy_checkout(Pool, nonblock).

%% @doc checkout_nonblock_next - Next state function
-spec checkout_nonblock_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
checkout_nonblock_next(S, Value, [_Pool]) ->
    checkout_next_(S, Value).

%% The next state is common for block/unblock
checkout_next_(S, Value) ->
    case do_checkout(S) of
        false -> S;
        {WId, WPid0} ->
            %% If it is the first time we use this worker 'bind' the Pid
            WPid = case WPid0 of undefined -> Value; _ -> WPid0 end,
            S#state{ checked_out = S#state.checked_out ++ [{WId, WPid}],
                     workers = tl(S#state.workers) };
        tmp_worker ->
          S#state{ checked_out = S#state.checked_out ++ [{tmp_worker, Value}] }
    end.

%% @doc checkout_nonblock_post - Postcondition for checkout_nonblock
-spec checkout_nonblock_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
checkout_nonblock_post(S, [_Pool], Worker) ->
  case do_checkout(S) of
    false                  -> tag(checkout_nonblock, eq(Worker, full));
    {_WorkerId, undefined} -> tag(checkout_nonblock, is_worker(Worker));
    {_WorkerId, Worker1}   -> tag(checkout_nonblock, eq(Worker, Worker1));
    tmp_worker             -> tag(checkout_nonblock, is_worker(Worker))
  end.

%% --- Operation: checkout_block ---
%% @doc checkout_block_args - Argument generator
-spec checkout_block_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
checkout_block_args(#state{pid=Pool}) ->
    [Pool].

%% @doc checkout_block - The actual operation
checkout_block(Pool) ->
    proxy_checkout(Pool, block).

%% @doc checkout_block_next - Next state function
-spec checkout_block_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
checkout_block_next(S, Value, [_Pool]) ->
    checkout_next_(S, Value).

%% @doc checkout_block_post - Postcondition for checkout_block
-spec checkout_block_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
checkout_block_post(S, [_Pool], Worker) ->
  case do_checkout(S) of
    false                  -> tag(checkout_block, is_exit(Worker));
    {_WorkerId, undefined} -> tag(checkout_block, is_worker(Worker));
    {_WorkerId, Worker1}   -> tag(checkout_block, eq(Worker, Worker1));
    tmp_worker             -> tag(checkout_block, is_worker(Worker))
  end.

%% @doc checkout_block_blocking - Is the operation blocking in this State
-spec checkout_block_blocking(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
checkout_block_blocking(S, [_Pool]) ->
    %% blocking checkout can block if we expect a checkout to fail
    not checkout_ok(S).

%% --- Operation: checkin ---
%% @doc checkin_pre/1 - Precondition for generation
-spec checkin_pre(S :: eqc_statem:symbolic_state()) -> boolean().
checkin_pre(S) ->
    S#state.checked_out /= [].

%% @doc checkin_args - Argument generator
-spec checkin_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
checkin_args(#state{pid=Pool, checked_out=CheckedOut}) ->
    [Pool,
     fault({{worker, -1}, {call, ?MODULE, spawn_process, []}}, elements(CheckedOut))].

%% @doc checkin_pre/2 - Precondition for checkin
-spec checkin_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
checkin_pre(S, [_Pool, Worker]) ->
    lists:member(Worker, S#state.checked_out).

%% @doc checkin - The actual operation
checkin(Pool, {_WId, WPid}) ->
    Res = proxy_checkin(Pool, WPid),
    gen_fsm:sync_send_all_state_event(Pool, get_avail_workers),
    Res.

%% @doc checkin_next - Next state function
-spec checkin_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
checkin_next(S, _Value, [_Pool, Worker = {tmp_worker, _}]) ->
  S#state{checked_out = S#state.checked_out -- [Worker] };
checkin_next(S, _Value, [_Pool, Worker = {WId, _}]) ->
  case [ Tmp || Tmp = {tmp_worker, _} <- S#state.checked_out ] of
    [] ->
      S#state{ checked_out = S#state.checked_out -- [Worker],
               workers = S#state.workers ++ [Worker] };

    [Tmp = {tmp_worker, WPid} | _] -> %% Promote the tmp_worker to WorkerId
      S#state{ checked_out = (S#state.checked_out -- [Tmp, Worker]) ++ [{WId, WPid}] }
  end.

%% @doc checkin_post - Postcondition for checkin
-spec checkin_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
checkin_post(_S, [_Pool, _Worker], Res) ->
    tag(checkin, eq(Res, ok)).

%% --- Operation: kill_worker ---
%% @doc kill_worker_pre/1 - Precondition for generation
-spec kill_worker_pre(S :: eqc_statem:symbolic_state()) -> boolean().
kill_worker_pre(S) ->
    S#state.checked_out /= [].

%% @doc kill_worker_args - Argument generator
-spec kill_worker_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
kill_worker_args(S) ->
    [elements(S#state.checked_out)].

%% @doc kill_worker_pre/2 - Precondition for kill_worker
-spec kill_worker_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
kill_worker_pre(S, [Worker]) ->
    lists:member(Worker, S#state.checked_out).

%% @doc kill_worker - The actual operation
kill_worker({_WId, WPid}) ->
    exit(WPid, kill),
    timer:sleep(1),
    WPid.

%% @doc kill_worker_next - Next state function
-spec kill_worker_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
kill_worker_next(S, _Value, [Worker = {tmp_worker, _}]) ->
  S#state{ checked_out = S#state.checked_out -- [Worker] };
kill_worker_next(S, _Value, [Worker = {WId, _}]) ->
  case [ Tmp || Tmp = {tmp_worker, _} <- S#state.checked_out ] of
    [] ->
      S#state{ checked_out = S#state.checked_out -- [Worker],
               workers = S#state.workers ++ [{WId, undefined}] };

    [Tmp = {tmp_worker, WPid} | _] -> %% Promote the tmp_worker to WorkerId
      S#state{ checked_out = (S#state.checked_out -- [Tmp, Worker]) ++ [{WId, WPid}] }
  end.

%% --- Operation: kill_idle_worker ---
%% @doc kill_idle_worker_pre/1 - Precondition for generation
-spec kill_idle_worker_pre(S :: eqc_statem:symbolic_state()) -> boolean().
kill_idle_worker_pre(S) ->
    idle_workers(S) /= [].

%% @doc kill_idle_worker_args - Argument generator
-spec kill_idle_worker_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
kill_idle_worker_args(S) ->
    [elements(idle_workers(S))].

%% @doc kill_idle_worker_pre/2 - Precondition for kill_idle_worker
-spec kill_idle_worker_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
kill_idle_worker_pre(S, [Worker]) ->
    lists:member(Worker, idle_workers(S)).

%% @doc kill_idle_worker - The actual operation
kill_idle_worker({_WId, WPid}) ->
    exit(WPid, kill),
    timer:sleep(1).

%% @doc kill_idle_worker_next - Next state function
-spec kill_idle_worker_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
kill_idle_worker_next(S, _Value, [Worker = {WId, _}]) ->
    S#state{workers = (S#state.workers -- [Worker]) ++ [{WId, undefined}] }.

%% --- Operation: spurious_exit ---
%% @doc spurious_exit_args - Argument generator
-spec spurious_exit_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
spurious_exit_args(#state{pid=Pool}) ->
    [Pool].

%% @doc spurious_exit - The actual operation
spurious_exit(Pool) ->
    Pid = spawn_linked_process(Pool),
    exit(Pid, kill).

%% -- Property ---------------------------------------------------------------
%% @doc <i>Optional callback</i>, Invariant, checked for each visited state
%%      during test execution.
%% -spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
invariant(S = #state{pid=Pool},_) when Pool /= undefined ->
    State = if length(S#state.checked_out) == S#state.size + S#state.max_overflow ->
                    full;
               length(S#state.checked_out) >= S#state.size ->
                    overflow;
               true ->
                    ready
            end,

    Workers = max(0, S#state.size - length(S#state.checked_out)),
    OverFlow = max(0, length(S#state.checked_out) - S#state.size),
    Monitors = length(S#state.checked_out),

    RealStatus = gen_fsm:sync_send_all_state_event(Pool, status),
    case RealStatus == {State, Workers, OverFlow, Monitors} of
        true ->
            true;
        _ ->
            {wrong_state, RealStatus, {State, Workers, OverFlow, Monitors}}
    end;
invariant(_, _) ->
    true.

%% @doc weight/2 - Distribution of calls
-spec weight(S, Cmd) -> integer()
    when S   :: eqc_statem:symbolic_state(),
         Cmd :: atom().
weight(_S, checkin)           -> 12;
weight(_S, checkout_block)    -> 10;
weight(_S, checkout_nonblock) -> 10;
weight(_S, kill_worker)       -> 7;
weight(_S, kill_idle_worker)  -> 5;
weight(_S, spurious_exit)     -> 4;
weight(_S, _Cmd)              -> 1.


prop_sequential() ->
    fault_rate(1, 10,
    ?FORALL(Cmds,commands(?MODULE),
    ?TRAPEXIT(
    begin
      worker_proxy_start(),
      HSR={_H ,_S, Res} = run_commands(?MODULE,Cmds),
      catch(stop_poolboy(whereis(?MODULE))),
      worker_proxy_stop(),
      aggregate(command_names(Cmds),
        pretty_commands(?MODULE, Cmds, HSR, Res == ok))
    end))).


prop_parallel() ->
    fault_rate(1, 10,
    ?FORALL(Cmds={Seq,Par},parallel_commands(?MODULE),
    ?TRAPEXIT(
    begin
      worker_proxy_start(),
      NewPar = Par,
      %% NewPar = [P ++ [{set, {var, 0}, {call, erlang, self, []}}] || P <- Par],
      HSR={_H,_S, Res} = run_parallel_commands(?MODULE,{Seq,NewPar}),
      catch(stop_poolboy(whereis(?MODULE))),
      worker_proxy_stop(),
      aggregate(command_names(Cmds),
        pretty_commands(?MODULE, {Seq, NewPar}, HSR, Res == ok))
    end))).


-ifdef(PULSE).
pulse_instrument() ->
  [ pulse_instrument(File) || File <- filelib:wildcard("../src/*.erl") ++
                                      filelib:wildcard("../test/*.erl") ].

pulse_instrument(File) ->
    io:format("compiling ~p~n", [File]),
    {ok, Mod} = compile:file(File, [{d, 'PULSE', true}, {d, 'EQC', true}, {d, 'TEST', true}]),

    code:purge(Mod),
    code:load_file(Mod),
    Mod.

prop_pulse() ->
    ?SETUP(fun() -> N = erlang:system_flag(schedulers_online, 1),
                    fun() -> erlang:system_flag(schedulers_online, N) end
           end,
           ?FORALL(Cmds={Seq, Par}, parallel_commands(?MODULE),
                   ?TRAPEXIT(
                      ?PULSE(HSR={_, _, R},
                             begin
                                 NewPar = [P ++ [{set, {var, 0}, {call, erlang, self, []}}] || P <- Par],
                                 PULSEHSR = run_parallel_commands(?MODULE, {Seq, NewPar}),
                                 catch(stop_poolboy(whereis(?MODULE))),
                                 PULSEHSR
                             end,
                             aggregate(command_names(Cmds),
                                       pretty_commands(?MODULE, Cmds, HSR,
                                                    R == ok)))))).
-endif.

%% -- Helper functions -------------------------------------------------------

checkout_ok(S) ->
    do_checkout(S) /= false.

do_checkout(S = #state{ workers = Pool, checked_out = CheckedOut }) ->
    case Pool of
      [Worker | _] -> Worker;
      [] when length(CheckedOut) < S#state.size + S#state.max_overflow -> tmp_worker;
      _ -> false
    end.

idle_workers(#state{ workers = Workers }) ->
  [ W || W = {_, WorkerPid} <- Workers, WorkerPid /= undefined ].

tag(_, true) -> true;
tag(Tag, Val) -> {Tag, Val}.

is_worker(Pid) when is_pid(Pid) -> true;
is_worker(NotPid) -> {got, NotPid, expected, a_pid}.

is_exit({'EXIT', {timeout, _}}) -> true;
is_exit(NotExit) -> {got, NotExit, expected, {'EXIT', {timeout, '...'}}}.

spawn_process() ->
    {spawn(fun() ->
                timer:sleep(5000)
        end), self()}.

spawn_linked_process(Pool) ->
    Parent = self(),
    Pid = spawn(fun() ->
                    link(Pool),
                    Parent ! {linked, self()},
                    timer:sleep(5000)
            end),
    receive
        {linked, Pid} ->
            Pid
    end.

%% -- A proxy process for keeping the workers --------------------------------
worker_proxy_start() ->
  (catch exit(whereis(worker_proxy), kill)), timer:sleep(1),
  register(worker_proxy, spawn(fun() -> worker_proxy_loop() end)).

worker_proxy_stop() ->
  (catch exit(whereis(worker_proxy), shutdown)), timer:sleep(1).

proxy_checkin(Pool, Worker) ->
  proxy_rpc({checkin, Pool, Worker}).

proxy_checkout(Pool, Opt) ->
  proxy_rpc({checkout, Pool, Opt}).

proxy_rpc(Action) ->
  case whereis(worker_proxy) of
    undefined -> error({worker_proxy, undefined});
    Pid ->
      Ref = make_ref(),
      Pid ! {self(), Ref, Action},
      receive {Ref, Res} -> Res
      after 200 -> error({worker_proxy, timeout}) end
  end.

worker_proxy_loop() ->
  receive
    {From, Ref, Action} ->
      case Action of
        {checkout, Pool, block}    ->
          spawn_link(fun() -> worker_proxy_block(From, Ref, Pool) end);
        {checkout, Pool, nonblock} ->
          From ! {Ref, (catch poolboy:checkout(Pool, false))};
        {checkin, Pool, Worker}    ->
          From ! {Ref, (catch poolboy:checkin(Pool, Worker))}
      end,
      worker_proxy_loop()
  end.

worker_proxy_block(From, Ref, Pool) ->
  From ! {Ref, (catch poolboy:checkout(Pool, true, 100))},
  timer:sleep(10000).

-endif.
-endif.
