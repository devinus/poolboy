-module(poolboy_test_worker_delay).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(_Args) ->
    Current = ets:update_counter(error_counter, current, 1),
    Success = ets:update_counter(error_counter, success, 0),
    case Current rem Success of
        0 -> 
            gen_server:start_link(?MODULE, [], []);
        _ ->
            {error, fail}
    end.

init([]) ->
    {ok, undefined}.

handle_call(die, _From, State) ->
    {stop, {error, died}, dead, State};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
