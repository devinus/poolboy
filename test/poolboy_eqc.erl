-module(poolboy_eqc).
-compile([export_all]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include_lib("eunit/include/eunit.hrl").

poolboy_test_() ->
	{timeout, 30,
		fun() ->
				?assert(eqc:quickcheck(eqc:testing_time(29, poolboy_eqc:prop_sequential())))
		end
	}.

-record(state,
	{
		pid,
		size,
		max_overflow,
		checked_out = []
	}).

initial_state() ->
	#state{}.

command(S) ->
	oneof(
		[{call, ?MODULE, start_poolboy, make_args(S, nat(), nat())} || S#state.pid == undefined] ++
			[{call, ?MODULE, stop_poolboy, [S#state.pid]} || S#state.pid /= undefined] ++
			[{call, ?MODULE, checkout_nonblock, [S#state.pid]} || S#state.pid /= undefined] ++
			[{call, ?MODULE, checkin, [S#state.pid, elements(S#state.checked_out)]} || S#state.pid /= undefined, S#state.checked_out /= []]
	).

make_args(_S, Size, Overflow) ->
	[[{size, Size}, {max_overflow, Overflow}, {worker_module, poolboy_test_worker}, {name, {local, poolboy_eqc}}]].

start_poolboy(Args) ->
	{ok, Pid} = poolboy:start_link(Args),
	Pid.

stop_poolboy(Pid) ->
	gen_fsm:sync_send_all_state_event(Pid, stop),
	timer:sleep(1).

checkout_nonblock(Pool) ->
	poolboy:checkout(Pool, false).

checkin(Pool, Worker) ->
	Res = poolboy:checkin(Pool, Worker),
	gen_fsm:sync_send_all_state_event(Pool, get_avail_workers),
	Res.

precondition(S,{call,_,start_poolboy,_}) ->
	%% only start new pool when old one is stopped
  S#state.pid == undefined;
precondition(S,_) when S#state.pid == undefined ->
	%% all other states need a running pool
	false;
precondition(S, {call, _, checkin, [_Pool, Pid]}) ->
	lists:member(Pid, S#state.checked_out);
precondition(_S,{call,_,_,_}) ->
	true.

postcondition(S,{call,_,checkout_nonblock,[_Pool]},R) ->
	case R of
		full ->
			length(S#state.checked_out) >= S#state.size + S#state.max_overflow;
		_ ->
			length(S#state.checked_out) < S#state.size + S#state.max_overflow
	end;
postcondition(_S, {call,_,checkin,_}, R) ->
	R == ok;
postcondition(_S,{call,_,_,_},_R) ->
	true.

next_state(S,V,{call,_,start_poolboy, [Args]}) ->
	S#state{pid=V,
		size=proplists:get_value(size, Args),
		max_overflow=proplists:get_value(max_overflow, Args)
	};
next_state(S,_V,{call,_,stop_poolboy, [_Args]}) ->
	S#state{pid=undefined, checked_out=[]}; 
next_state(S,V,{call,_,checkout_nonblock, _}) ->
	case checkout_ok(S) of
		false ->
			S;
		_ ->
			S#state{checked_out=S#state.checked_out++[V]}
	end;
next_state(S,_V,{call, _, checkin, [_Pool, Worker]}) ->
	S#state{checked_out=S#state.checked_out -- [Worker]}.


prop_sequential() ->
		?FORALL(Cmds,commands(?MODULE),
			?TRAPEXIT(
				aggregate(command_names(Cmds), 
					begin
							{H,S,Res} = run_commands(?MODULE,Cmds),
							catch(stop_poolboy(whereis(poolboy_eqc))),
							?WHENFAIL(io:format("History: ~p\nState: ~p\nRes: ~p\n~p\n",
									[H,S,Res, zip(tl(Cmds), [Y || {_, Y} <- H])]),
								Res == ok)
					end))).

checkout_ok(S) ->
	length(S#state.checked_out) < S#state.size + S#state.max_overflow.

-endif.
-endif.
