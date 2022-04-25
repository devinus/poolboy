%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_sup).
-behaviour(supervisor).

-export([start_link/2, init/1]).

-spec start_link(Mod, Args) -> Result when
	Mod :: module(),
	Args :: term(),
	Result :: {ok, pid()} | ignore | {error, StartlinkError},
	StartlinkError :: {already_started, pid()} | {shutdown, term()} | term().
start_link(Mod, Args) ->
    supervisor:start_link(?MODULE, {Mod, Args}).

%% @private

-spec init(Tuple) -> Result when 
	Tuple :: {Mod, Args},
	Mod :: module(),
	Args :: term(),
	Result :: {ok,{SupFlags, ChildSpec}} | ignore,
	SupFlags :: {simple_one_for_one, 0, 1},
	ChildSpec :: [supervisor:child_spec()].
init({Mod, Args}) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{Mod, {Mod, start_link, [Args]},
            temporary, 5000, worker, [Mod]}]}}.
