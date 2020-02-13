-module(poolboy_worker_supervisor).

-callback start_child() -> {ok, Pid} |
                           {error, Reason} when
    Pid    :: pid(),
    Reason :: term().
