# Poolboy - A hunky Erlang worker pool factory

[![Build Status](https://api.travis-ci.org/devinus/poolboy.svg?branch=master)](https://travis-ci.org/devinus/poolboy)

[![Support via Gratipay](https://cdn.rawgit.com/gratipay/gratipay-badge/2.3.0/dist/gratipay.png)](https://gratipay.com/devinus/)

Poolboy is a **lightweight**, **generic** pooling library for Erlang with a
focus on **simplicity**, **performance**, and **rock-solid** disaster recovery.

## Usage
The most basic use case is to check out a worker, make a call and manually 
return it to the pool when done
```erl-sh
1> Worker = poolboy:checkout(PoolName).
<0.9001.0>
2> gen_server:call(Worker, Request).
ok
3> poolboy:checkin(PoolName, Worker).
ok
```
Alternatively you can use a transaction which will return the worker to the 
pool when the call is finished.
```erl-sh
poolboy:transaction(
    PoolName,
    fun(Worker) -> gen_server:call(Worker, Request) end, 
    TransactionTimeout
)
```
## Example

This is an example application showcasing database connection pools using
Poolboy and [epgsql](https://github.com/epgsql/epgsql).

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
                {hostname, "127.0.0.1"},
                {database, "db1"},
                {username, "db1"},
                {password, "abc123"}
            ]},
            {pool2, [
                {size, 5},
                {max_overflow, 10}
			], [
                {hostname, "127.0.0.1"},
                {database, "db2"},
                {username, "db2"},
                {password, "abc123"}
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

-export([start/0, stop/0, squery/2, equery/3]).
-export([start/2, stop/1]).
-export([init/1]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start(_Type, _Args) ->
    supervisor:start_link({local, example_sup}, ?MODULE, []).

stop(_State) ->
    ok.

init([]) ->
    {ok, Pools} = application:get_env(example, pools),
    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {local, Name}},
            		{worker_module, example_worker}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

squery(PoolName, Sql) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {squery, Sql})
    end).

equery(PoolName, Stmt, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {equery, Stmt, Params})
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

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    {ok, Conn} = epgsql:connect(Hostname, Username, Password, [
        {database, Database}
    ]),
    {ok, #state{conn=Conn}}.

handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, epgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    {reply, epgsql:equery(Conn, Stmt, Params), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = epgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
```

## Pool Options

- `name`: the pool name - optional
- `worker_module`: the module that represents the workers - mandatory
- `size`: maximum pool size - optional
- `max_overflow`: maximum number of workers created if pool is empty - optional
- `strategy`: `lifo` or `fifo`, determines whether checked in workers should be
  placed first or last in the line of available workers. Default is `lifo`.
- `overflow_ttl`: time in milliseconds you want to wait before removing overflow
  workers. Useful when it's expensive to start workers. Default is 0.
- `overflow_check_period`: time in milliseconds for checking overflow workers to rip. 
  Default is min(1 sec, overflow_ttl). Cheking job will not be started, if overflow_ttl is 0.
  
## Pool Status
Returns : {Status, Workers, Overflow, InUse}
- `Status`: ready | full | overflow
            The ready atom indicates there are workers that are not checked out 
            ready. The full atom indicates all workers including overflow are 
            checked out. The overflow atom is used to describe the condition 
            when all permanent workers are in use but there is overflow capacity 
            available.
- `Workers`: Number of workers ready for use.
- `Overflow`: Number of overflow workers started, should never exceed number 
              specified by MaxOverflow when starting pool
- `InUse`: Number of workers currently busy/checked out

## Authors

- Devin Torres (devinus) <devin@devintorres.com>
- Andrew Thompson (Vagabond) <andrew@hijacked.us>
- Kurt Williams (onkel-dirtus) <kurt.r.williams@gmail.com>

## License

Poolboy is available in the public domain (see `UNLICENSE`).
Poolboy is also optionally available under the ISC license (see `LICENSE`),
meant especially for jurisdictions that do not recognize public domain works.
