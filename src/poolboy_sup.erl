%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_sup).
-behaviour(supervisor).

-export([start_link/2, init/1]).
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{supervisor, pulse_supervisor}]}).
-endif.

start_link(Mod, Args) ->
    supervisor:start_link(?MODULE, {Mod, Args}).

init({Mod, Args}) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{Mod, {Mod, start_link, [Args]},
            temporary, 5000, worker, [Mod]}]}}.
