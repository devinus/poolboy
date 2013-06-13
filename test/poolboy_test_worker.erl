-module(poolboy_test_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init([]) ->
    {ok, undefined};
init([{locker, Locker} | Args]) ->
    %% this is used by backoff tests to emulate database(or other resource)
    %% slownest
    Locker ! {may_i_proceed, self()},
    receive
        ok ->
            init(Args);
        Else ->
            {error, Else}
    after 200 ->
        {error, timeout}
    end;
init([_A | Args]) ->
    init(Args).

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
