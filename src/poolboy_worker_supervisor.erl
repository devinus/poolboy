-module(poolboy_worker_supervisor).

-callback start_child() -> {ok, Pid} |
                           {error, Reason} when
    Pid    :: pid(),
    Reason :: term().

-callback start_child(integer()) -> {ok, Pid} |
                                    {error, Reason} when
    Pid    :: pid(),
    Reason :: term().

-optional_callbacks([start_child/0, start_child/1]).
