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
          checked_out = []
        }).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Operations -------------------------------------------------------------

%% --- Operation: start_poolboy ---
%% @doc start_poolboy_pre/1 - Precondition for generation
-spec start_poolboy_pre(S :: eqc_statem:symbolic_state()) -> boolean().
start_poolboy_pre(#state{pid=undefined}) ->
    true;
start_poolboy_pre(_S) ->
    false.

%% @doc start_poolboy_args - Argument generator
-spec start_poolboy_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
start_poolboy_args(_S) ->
    ?LET({Size, Overflow}, {nat(), nat()},
         [[{size, Size},
           {max_overflow, Overflow},
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
start_poolboy_next(S, Pid, [Args]) ->
    S#state{pid=Pid,
            size=proplists:get_value(size, Args),
            max_overflow=proplists:get_value(max_overflow, Args)
           }.

%% --- Operation: stop_poolboy ---
%% @doc stop_poolboy_pre/1 - Precondition for generation
-spec stop_poolboy_pre(S :: eqc_statem:symbolic_state()) -> boolean().
stop_poolboy_pre(#state{pid=undefined}) ->
    false;
stop_poolboy_pre(_S) ->
    true.

%% @doc stop_poolboy_args - Argument generator
-spec stop_poolboy_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
stop_poolboy_args(#state{pid=Pid}) ->
    [Pid].

%% @doc stop_poolboy - The actual operation
stop_poolboy(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop),
    timer:sleep(1).

%% @doc stop_poolboy_next - Next state function
-spec stop_poolboy_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
stop_poolboy_next(S, _Value, [_Pid]) ->
    S#state{pid=undefined, checked_out=[]}.

%% --- Operation: checkout_nonblock ---
%% @doc checkout_nonblock_pre/1 - Precondition for generation
-spec checkout_nonblock_pre(S :: eqc_statem:symbolic_state()) -> boolean().
checkout_nonblock_pre(#state{pid=undefined}) ->
    false;
checkout_nonblock_pre(_S) ->
    true.

%% @doc checkout_nonblock_args - Argument generator
-spec checkout_nonblock_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
checkout_nonblock_args(#state{pid=Pid}) ->
    [Pid].

%% @doc checkout_nonblock - The actual operation
checkout_nonblock(Pid) ->
    {poolboy:checkout(Pid, false), self()}.

%% @doc checkout_nonblock_next - Next state function
-spec checkout_nonblock_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
checkout_nonblock_next(S, Value, [_Pid]) ->
    %% if the model says the checkout worked, store the result
    case checkout_ok(S) of
        false ->
            S;
        _ ->
            S#state{checked_out=S#state.checked_out++[Value]}
    end.

%% @doc checkout_nonblock_post - Postcondition for checkout_nonblock
-spec checkout_nonblock_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
checkout_nonblock_post(S, [_Pid], Res) ->
    case Res of
        {full, _} ->
            case length(S#state.checked_out) >= S#state.size + S#state.max_overflow of
                true ->
                    true;
                _ ->
                    {checkout_nonblock, Res}
            end;
        _ ->
            case length(S#state.checked_out) < S#state.size + S#state.max_overflow of
                true ->
                    true;
                _ ->
                    {checkout_block, Res}
            end
    end.
%% --- Operation: checkout_block ---
%% @doc checkout_block_pre/1 - Precondition for generation
-spec checkout_block_pre(S :: eqc_statem:symbolic_state()) -> boolean().
checkout_block_pre(#state{pid=undefined}) ->
    false;
checkout_block_pre(_S) ->
    true.

%% @doc checkout_block_args - Argument generator
-spec checkout_block_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
checkout_block_args(#state{pid=Pid}) ->
    [Pid].

%% @doc checkout_block - The actual operation
checkout_block(Pid) ->
    {catch(poolboy:checkout(Pid, true, 100)), self()}.

%% @doc checkout_block_next - Next state function
-spec checkout_block_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
checkout_block_next(S, Value, [_Pid]) ->
    %% if the model says the checkout worked, store the result
    case checkout_ok(S) of
        false ->
            S;
        _ ->
            S#state{checked_out=S#state.checked_out++[Value]}
    end.

%% @doc checkout_block_post - Postcondition for checkout_block
-spec checkout_block_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
checkout_block_post(S, [_Pid], Res) ->
    case Res of
        {{'EXIT', {timeout, _}}, _} ->
            case length(S#state.checked_out) >= S#state.size + S#state.max_overflow of
                true ->
                    true;
                _ ->
                    {checkout_block, Res}
            end;
        _ ->
            case length(S#state.checked_out) < S#state.size + S#state.max_overflow of
                true ->
                    true;
                _ ->
                    {checkout_block, Res}
            end
    end.

%% @doc checkout_block_blocking - Is the operation blocking in this State
-spec checkout_block_blocking(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
checkout_block_blocking(S, [_Pid]) ->
    %% blocking checkout can block if we expect a checkout to fail
    not checkout_ok(S).

%% --- Operation: checkin ---
%% @doc checkin_pre/1 - Precondition for generation
-spec checkin_pre(S :: eqc_statem:symbolic_state()) -> boolean().
checkin_pre(S) ->
    #state{pid=Pid, checked_out=CheckedOut} = S,
    Pid /=undefined andalso CheckedOut /= [].

%% @doc checkin_args - Argument generator
-spec checkin_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
checkin_args(#state{pid=Pid, checked_out=CheckedOut}) ->
    [Pid,
     fault({call, ?MODULE, spawn_process, []}, elements(CheckedOut))].

%% @doc checkin_pre/2 - Precondition for checkin
-spec checkin_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
checkin_pre(S, [_Pool, WorkerPid]) ->
    lists:member(WorkerPid, S#state.checked_out).

%% @doc checkin - The actual operation
checkin(Pid, {Worker, _}) ->
    Res = poolboy:checkin(Pid, Worker),
    gen_fsm:sync_send_all_state_event(Pid, get_avail_workers),
    Res.

%% @doc checkin_next - Next state function
-spec checkin_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
checkin_next(S, _Value, [_Pid, Worker]) ->
    S#state{checked_out=S#state.checked_out -- [Worker]}.

%% @doc checkin_post - Postcondition for checkin
-spec checkin_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
checkin_post(_S, [_Pid, _Proc], Res) ->
    case Res of
        ok ->
            true;
        _ ->
            {checkin, Res}
    end.

%% --- Operation: kill_worker ---
%% @doc kill_worker_pre/1 - Precondition for generation
-spec kill_worker_pre(S :: eqc_statem:symbolic_state()) -> boolean().
kill_worker_pre(S) ->
    #state{pid=Pid, checked_out=CheckedOut} = S,
    Pid /=undefined andalso CheckedOut /= [].

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
kill_worker({Worker, _}) ->
    exit(Worker, kill),
    timer:sleep(1),
    Worker.

%% @doc kill_worker_next - Next state function
-spec kill_worker_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
kill_worker_next(S, _Value, [Worker]) ->
    S#state{checked_out=S#state.checked_out -- [Worker]}.

%% --- Operation: kill_idle_worker ---
%% @doc kill_idle_worker_pre/1 - Precondition for generation
-spec kill_idle_worker_pre(S :: eqc_statem:symbolic_state()) -> boolean().
kill_idle_worker_pre(S) ->
    S#state.pid /= undefined andalso S#state.checked_out /= [].

%% @doc kill_idle_worker_args - Argument generator
-spec kill_idle_worker_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
kill_idle_worker_args(S) ->
    [S#state.pid].

%% @doc kill_idle_worker_pre/2 - Precondition for kill_idle_worker
-spec kill_idle_worker_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
kill_idle_worker_pre(S, [_Pool]) ->
    length(S#state.checked_out) < S#state.size.

%% @doc kill_idle_worker - The actual operation
kill_idle_worker(Pool) ->
    Pid = poolboy:checkout(Pool, false),
    case Pid of
        _ when is_pid(Pid) ->
            poolboy:checkin(Pool, Pid),
            kill_worker({Pid, self()});
        _ ->
            timer:sleep(1),
            kill_idle_worker(Pool)
    end.

%% --- Operation: spurious_exit ---
%% @doc spurious_exit_pre/1 - Precondition for generation
-spec spurious_exit_pre(S :: eqc_statem:symbolic_state()) -> boolean().
spurious_exit_pre(#state{pid=undefined}) ->
    false;
spurious_exit_pre(_S) ->
    true.

%% @doc spurious_exit_args - Argument generator
-spec spurious_exit_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
spurious_exit_args(#state{pid=Pid}) ->
    [Pid].

%% @doc spurious_exit - The actual operation
spurious_exit(Pool) ->
    Pid = spawn_linked_process(Pool),
    exit(Pid, kill).

next_state_common(S,V,{call, erlang, self, []}) ->
    %% added after test generation, values are never symbolic
    S#state{checked_out=[{Worker, Pid} || {Worker, Pid} <- S#state.checked_out, Pid /= V]};
next_state_common(S, _V, _Call) -> S.

%% -- Property ---------------------------------------------------------------
%% @doc <i>Optional callback</i>, Invariant, checked for each visited state
%%      during test execution.
%% -spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
invariant(S = #state{pid=Pid},_) when Pid /= undefined ->
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

    RealStatus = gen_fsm:sync_send_all_state_event(Pid, status),
    case RealStatus == {State, Workers, OverFlow, Monitors} of
        true ->
            true;
        _ ->
            {wrong_state, RealStatus, {State, Workers, OverFlow, Monitors}}
    end;
invariant(_,_) ->
    true.

%% @doc weight/2 - Distribution of calls
-spec weight(S, Cmd) -> integer()
    when S   :: eqc_statem:symbolic_state(),
         Cmd :: atom().
weight(_S, erlang_self) -> 0;
weight(_S, _Cmd) -> 1.


prop_sequential() ->
    fault_rate(1, 10,
               ?FORALL(Cmds,commands(?MODULE),
                       ?TRAPEXIT(
                          aggregate(command_names(Cmds),
                                    begin
                                        HSR={_H , _S,Res} = run_commands(?MODULE,Cmds),
                                        catch(stop_poolboy(whereis(?MODULE))),
                                        pretty_commands(?MODULE, Cmds, HSR,
                                                        Res == ok)
                                    end)))).


prop_parallel() ->
    fault_rate(1, 10,
               ?FORALL(Cmds={Seq,Par},parallel_commands(?MODULE),
                       ?TRAPEXIT(
                          aggregate(command_names(Cmds),
                                    begin
                                        NewPar = [P ++ [{set, {var, 0}, {call, erlang, self, []}}] || P <- Par],
                                        HSR={_H,_S, Res} = run_parallel_commands(?MODULE,{Seq,NewPar}),
                                        catch(stop_poolboy(whereis(?MODULE))),
                                        pretty_commands(?MODULE, Cmds, HSR,
                                                        Res == ok)
                                    end)))).


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

checkout_ok(S) ->
    length(S#state.checked_out) < S#state.size + S#state.max_overflow.

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

-endif.
-endif.
