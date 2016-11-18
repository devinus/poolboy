%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_worker).

-callback start_link(WorkerArgs) -> {ok, Pid} |
                                    {error, {already_started, Pid}} |
                                    {error, Reason} when
    WorkerArgs :: any(),
    Pid        :: pid(),
    Reason     :: term().
