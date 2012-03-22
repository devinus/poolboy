%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_worker).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{start_link, 1}];
behaviour_info(_Other) ->
    undefined.
