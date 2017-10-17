-module(example_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([warmup	/0]).

-record(state, {conn}).

start_link(Args) ->
    %io:fwrite("#MESSAGE : ~p calls example_worker:start_link()~n(default callback is example_worker:init())~n"),
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