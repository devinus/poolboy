# Poolboy - A hunky Erlang worker pool factory

[![Build Status](https://api.travis-ci.org/devinus/poolboy.svg?branch=master)](https://travis-ci.org/devinus/poolboy)

[![Support via Gratipay](https://cdn.rawgit.com/gratipay/gratipay-badge/2.3.0/dist/gratipay.png)](https://gratipay.com/devinus/)

Poolboy is a **lightweight**, **generic** pooling library for Erlang with a
focus on **simplicity**, **performance**, and **rock-solid** disaster recovery.

## Usage

```erl-sh
1> Worker = poolboy:checkout(PoolName).
<0.9001.0>
2> gen_server:call(Worker, Request).
ok
3> poolboy:checkin(PoolName, Worker).
ok
```

## Example

This is a very simple Poolboy example showcasing the call flow. Print statements at various checkpoints and dummy call of workers (doing nothing actually) is what we aim to demonstrate in this example.

### example.app

```erlang
{application, example, [
    {description, "An example application"},
    {vsn, "0.1"},
    {applications, [kernel, stdlib, sasl, crypto, ssl]},
    {modules, [example, example_worker]},
    {registered, [example]},
    {mod, {example, []}},
    {env, [
        {pools, [
            {pool1, [
                {size, 10},
                {max_overflow, 20}
            ], [
                {hostname, "127.0.0.1/login.php"},
                {database, "database"},
                {username, "root"},
                {password, "rootpassword"}
            ]},
            {pool2, [
                {size, 5},
                {max_overflow, 10}
            ], [
                {hostname, "127.0.0.1/login.php"},
                {database, "database"},
                {username, "root"},
                {password, "rootpassword"}
            ]}
        ]}
    ]}
]}.
```

### example.erl

```erlang
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
```

### example_worker.erl

```erlang
-module(example_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([warmup	/0]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    io:fwrite("#MESSAGE : ~p defines a new worker~n", [self()]),
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    io:fwrite("#MESSAGE : ~p will be starting to warmup~n", [self()]),
    {ok} = warmup(),
    io:fwrite("#MESSAGE : ~p warmup is complete~n~n~n", [self()]),
    {ok, ok}.

warmup() ->
    io:fwrite("#MESSAGE : ~p is now warming up~n", [self()]),
    {ok}.

handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    io:fwrite("lets handle a squery call~n"),
    {reply, epgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    io:fwrite("lets handle a equery call~n"),
    {reply, epgsql:equery(Conn, Stmt, Params), State};
handle_call(_Request, _From, State) ->
    io:fwrite("call handling success~n"),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    io:fwrite("lets handle a cast (async call)~n"),
    {noreply, State}.

handle_info(_Info, State) ->
    io:fwrite("lets example_worker:handle_info~n"),
    {noreply, State}.

terminate(_Reason, ok) ->
    io:fwrite("#MESSAGE : ~p wants to terminate the worker~n", [self()]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
```
## Example usage

```erl-sh
1> example:start().
--- % start the supervisors, initiate the workers
2> example:squery(pool1, hello).
--- % get the workers to work : (synchronous) call
3> example:equery(pool1, hello, world).
--- % get the workers to work : (synchronous) call
4> example:xquery(pool1, hello, world).
--- % get the workers to work : (asynchronous) cast
5> example:stop().
--- % kill the workers and end the pool
```
Find the complete terminal screenshots along with stats and explanation [here](https://github.com/rounakdatta/poolboy/edit/master/report.pdf).

## Options

- `name`: the pool name
- `worker_module`: the module that represents the workers
- `size`: maximum pool size
- `max_overflow`: maximum number of workers created if pool is empty
- `strategy`: `lifo` or `fifo`, determines whether checked in workers should be
  placed first or last in the line of available workers. So, `lifo` operates like a traditional stack; `fifo` like a queue. Default is `lifo`.

## Authors

- Devin Torres (devinus) <devin@devintorres.com>
- Andrew Thompson (Vagabond) <andrew@hijacked.us>
- Kurt Williams (onkel-dirtus) <kurt.r.williams@gmail.com>

## License

Poolboy is available in the public domain (see `UNLICENSE`).
Poolboy is also optionally available under the ISC license (see `LICENSE`),
meant especially for jurisdictions that do not recognize public domain works.
