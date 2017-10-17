-module(example).
-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0, squery/2, equery/3, xquery/3]).
-export([start/2, stop/1]).
-export([init/1]).

start() ->
	io:fwrite("#MESSAGE : ~p calls example:start()~n", [self()]),
    application:start(?MODULE).

stop() ->
	io:fwrite("#MESSAGE : ~p calls example:stop()~n", [self()]),
    application:stop(?MODULE).

start(_Type, _Args) ->
	io:fwrite("#MESSAGE : ~p calls supervisor:start_link()~n(default callback is example:init())~n", [self()]),
    supervisor:start_link({local, example_sup}, ?MODULE, []).

stop(_State) ->
    ok.

init([]) ->
	io:fwrite("#MESSAGE : ~p called example:init()~n", [self()]),
    {ok, Pools} = application:get_env(example, pools),
    io:fwrite("#MESSAGE : ~p defining PoolSpecs~n", [self()]),
    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
    	io:fwrite("#MESSAGE : ~p defining PoolArgs~n", [self()]),
        PoolArgs = [{name, {local, Name}},
            		{worker_module, example_worker}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),
    io:fwrite("#MESSAGE : ~p example:init() call is now complete~n", [self()]),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

squery(PoolName, Sql) ->
	io:fwrite("i called squery call~n"),
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {squery, Sql})
    end).

equery(PoolName, Stmt, Params) ->
	io:fwrite("i called equery call~n"),
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {equery, Stmt, Params})
    end).

xquery(PoolName, Stmt, Params) ->
	io:fwrite("i called xquery cast~n"),
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:cast(Worker, {xquery, Stmt, Params})
    end).