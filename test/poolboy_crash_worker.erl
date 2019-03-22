-module(poolboy_crash_worker).
-behaviour(poolboy_worker).

-export([start_link/1]).

start_link(_Args) ->
    {error, not_starting}.
