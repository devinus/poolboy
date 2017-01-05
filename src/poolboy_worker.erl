%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_worker).

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-endif.

-callback start_link(WorkerArgs) -> {ok, Pid} |
                                    {error, {already_started, Pid}} |
                                    {error, Reason} when
    WorkerArgs :: proplists:proplist(),
    Pid        :: pid(),
    Reason     :: term().
