%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_sup).
-behaviour(supervisor).

-export([start_link/2, init/1]).

start_link(Mod, Args) ->
    supervisor:start_link(?MODULE, {Mod, Args}).

init({_Mod, {dynamic, _Args}}) ->
    % arguments are dynamic for each worker, then each worker must
    % be created dynamically
    {ok, { {one_for_one, 0, 1}, []} };
init({Mod, Args})           ->
    {ok, {{simple_one_for_one, 0, 1},
          [{Mod, {Mod, start_link, [Args]},
            temporary, 5000, worker, [Mod]}]}}.
