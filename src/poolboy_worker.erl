%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_worker).

-export([behaviour_info/1]).
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-endif.

behaviour_info(callbacks) ->
    [{start_link, 1}];
behaviour_info(_Other) ->
    undefined.
