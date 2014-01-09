%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_worker).

-ifdef(have_callback_support).

-callback start_link(WorkerArgs) -> {ok, Pid} |
                                    {error, {already_started, Pid}} |
                                    {error, Reason} when
    WorkerArgs :: proplists:proplist(),
    Pid        :: pid(),
    Reason     :: term().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{start_link, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.
