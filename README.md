Poolboy - A hunky Erlang worker pool factory
============================================

Usage
-----

```erlang
Worker = poolboy:checkout(PoolName),
Reply = gen_server:call(Worker, WorkerFun),
poolboy:checkin(PoolName, Worker),
Reply.
```

Example Application
-------------------

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
                {max_overflow, 20},
                {hostname, "127.0.0.1"},
                {database, "db1"},
                {username, "db1"},
                {password, "abc123"}
            ]},
            {pool2, [
                {size, 5},
                {max_overflow, 10},
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

-export([start/0, stop/0, start/2, stop/1, init/1, squery/2, equery/3]).

start() -> application:start(?MODULE).
stop()  -> application:stop(?MODULE).

start(_Type, _Args) ->
    supervisor:start_link({local, example_sup}, ?MODULE, []).
stop(_State) -> ok.

init([]) ->
    {ok, Pools} = application:get_env(example, pools),
    PoolSpecs = lists:map(fun({PoolName, PoolConfig}) ->
        Args = [{name, {local, PoolName}},
                {worker_module, example_worker}]
                ++ PoolConfig,
        {PoolName, {poolboy, start_link, [Args]},
                    permanent, 5000, worker, [poolboy]}
    end, Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

squery(PoolName, Sql) ->
    Worker = poolboy:checkout(PoolName),
    Reply = gen_server:call(Worker, {squery, Sql}),
    poolboy:checkin(PoolName, Worker),
    Reply.

equery(PoolName, Stmt, Params) ->
    Worker = poolboy:checkout(PoolName),
    Reply = gen_server:call(Worker, {equery, Stmt, Params}),
    poolboy:checkin(PoolName, Worker),
    Reply.
```

### example_worker.erl

```erlang
-module(example_worker).
-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {conn}).

start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop() -> gen_server:cast(?MODULE, stop).

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    {ok, Conn} = pgsql:connect(Hostname, Username, Password, [
        {database, Database}
    ]),
    {ok, #state{conn=Conn}}.

handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:equery(Conn, Stmt, Params), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    {stop, shutdown, State};
handle_info({'EXIT', _, _}, State) ->
    {stop, shutdown, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    pgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
```

Options
-------

- `name`: the pool name
- `worker_module`: the module that represents the workers
- `size`: maximum pool size
- `max_overflow`: maximum number of workers created if pool is empty

Authors
-------
- Devin Torres (devinus) <devin@devintorres.com>
- Andrew Thompson (Vagabond) <andrew@hijacked.us>
- Kurt Williams (onkel-dirtus) <kurt.r.williams@gmail.com>

License
-------
Poolboy is available in the public domain (see `UNLICENSE`).
Poolboy is also optionally available under the Apache License (see `LICENSE`),
meant especially for jurisdictions that do not recognize public domain works.
