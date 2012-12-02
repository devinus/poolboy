-module(poolboy_tests).

-include_lib("eunit/include/eunit.hrl").

-define(sync(Pid, Event),
    gen_server:call(Pid, Event)).

pool_test_() ->
    {foreach,
        fun() ->
            error_logger:tty(false)
        end,
        fun(_) ->
            case whereis(poolboy_test) of
                undefined -> ok;
                Pid -> ?sync(Pid, stop)
            end,
            error_logger:tty(true)
        end,
        [
            {<<"Basic pool operations">>,
                fun pool_startup/0
            },
            {<<"Pool overflow should work">>,
                fun pool_overflow/0
            },
            {<<"Pool behaves when empty">>,
                fun pool_empty/0
            },
            {<<"Pool behaves when empty and oveflow is disabled">>,
                fun pool_empty_no_overflow/0
            },
            {<<"Pool behaves on worker death">>,
                fun worker_death/0
            },
            {<<"Pool behaves when full and a worker dies">>,
                fun worker_death_while_full/0
            },
            {<<"Pool behaves when full, a worker dies and overflow disabled">>,
                fun worker_death_while_full_no_overflow/0
            },
            {<<"Non-blocking pool behaves when full and overflow disabled">>,
                fun pool_full_nonblocking_no_overflow/0
            },
            {<<"Non-blocking pool behaves when full">>,
                fun pool_full_nonblocking/0
            },
            {<<"Pool behaves on owner death">>,
                fun owner_death/0
            },
            {<<"Worker checked-in after an exception in a transaction">>,
                fun checkin_after_exception_in_transaction/0
            },
            {<<"Pool returns status">>,
                fun pool_returns_status/0
            }
        ]
    }.

%% Tell a worker to exit and await its impending doom.
kill_worker(Pid) ->
    erlang:monitor(process, Pid),
    gen_server:call(Pid, die),
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    end.

checkin_worker(Pid, Worker) ->
    %% There's no easy way to wait for a checkin to complete, because it's
    %% async and the supervisor may kill the process if it was an overflow
    %% worker. The only solution seems to be a nasty hardcoded sleep.
    poolboy:checkin(Pid, Worker),
    timer:sleep(500).

pool_startup() ->
    %% Check basic pool operation.
    {ok, Pid} = new_pool(10, 5),
    ?assertEqual(10, length(?sync(Pid, get_avail_workers))),
    poolboy:checkout(Pid),
    ?assertEqual(9, length(?sync(Pid, get_avail_workers))),
    Worker = poolboy:checkout(Pid),
    ?assertEqual(8, length(?sync(Pid, get_avail_workers))),
    checkin_worker(Pid, Worker),
    ?assertEqual(9, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(1, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

pool_overflow() ->
    %% Check that the pool overflows properly.
    {ok, Pid} = new_pool(5, 5),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(7, length(?sync(Pid, get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(0, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

pool_empty() ->
    %% Checks that the the pool handles the empty condition correctly when
    %% overflow is enabled.
    {ok, Pid} = new_pool(5, 2),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(7, length(?sync(Pid, get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    Self = self(),
    spawn(fun() ->
        Worker = poolboy:checkout(Pid),
        Self ! got_worker,
        checkin_worker(Pid, Worker)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(0, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

pool_empty_no_overflow() ->
    %% Checks the pool handles the empty condition properly when overflow is
    %% disabled.
    {ok, Pid} = new_pool(5, 0),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    [A, B, C, D, E] = Workers,
    Self = self(),
    spawn(fun() ->
        Worker = poolboy:checkout(Pid),
        Self ! got_worker,
        checkin_worker(Pid, Worker)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(2, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(4, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(0, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

worker_death() ->
    %% Check that dead workers are only restarted when the pool is not full
    %% and the overflow count is 0. Meaning, don't restart overflow workers.
    {ok, Pid} = new_pool(5, 2),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(7, length(?sync(Pid, get_all_workers))),
    kill_worker(A),
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(6, length(?sync(Pid, get_all_workers))),
    kill_worker(B),
    kill_worker(C),
    ?assertEqual(1, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(4, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

worker_death_while_full() ->
    %% Check that if a worker dies while the pool is full and there is a
    %% queued checkout, a new worker is started and the checkout serviced.
    %% If there are no queued checkouts, a new worker is not started.
    {ok, Pid} = new_pool(5, 2),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    [A, B|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(7, length(?sync(Pid, get_all_workers))),
    Self = self(),
    spawn(fun() ->
        poolboy:checkout(Pid),
        Self ! got_worker,
        %% XXX: Don't release the worker. We want to also test what happens
        %% when the worker pool is full and a worker dies with no queued
        %% checkouts.
        timer:sleep(5000)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    kill_worker(A),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    kill_worker(B),
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(6, length(?sync(Pid, get_all_workers))),
    ?assertEqual(6, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

worker_death_while_full_no_overflow() ->
    %% Check that if a worker dies while the pool is full and there's no
    %% overflow, a new worker is started unconditionally and any queued
    %% checkouts are serviced.
    {ok, Pid} = new_pool(5, 0),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    Self = self(),
    spawn(fun() ->
        poolboy:checkout(Pid),
        Self ! got_worker,
        %% XXX: Do not release, need to also test when worker dies and no
        %% checkouts queued.
        timer:sleep(5000)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    kill_worker(A),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    kill_worker(B),
    ?assertEqual(1, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    kill_worker(C),
    ?assertEqual(2, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(3, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

pool_full_nonblocking_no_overflow() ->
    %% Check that when the pool is full, checkouts return 'full' when the
    %% option to use non-blocking checkouts is used.
    {ok, Pid} = new_pool(5, 0),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    ?assertEqual(A, poolboy:checkout(Pid)),
    ?assertEqual(5, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

pool_full_nonblocking() ->
    %% Check that when the pool is full, checkouts return 'full' when the
    %% option to use non-blocking checkouts is used.
    {ok, Pid} = new_pool(5, 5),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 9)],
    ?assertEqual(0, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(10, length(?sync(Pid, get_all_workers))),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    NewWorker = poolboy:checkout(Pid, false),
    ?assertEqual(false, is_process_alive(A)), %% Overflow workers get shutdown
    ?assert(is_pid(NewWorker)),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    ?assertEqual(10, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

owner_death() ->
    %% Check that a dead owner (a process that dies with a worker checked out)
    %% causes the pool to dismiss the worker and prune the state space.
    {ok, Pid} = new_pool(5, 5),
    spawn(fun() ->
        poolboy:checkout(Pid),
        receive after 500 -> exit(normal) end
    end),
    timer:sleep(1000),
    ?assertEqual(5, length(?sync(Pid, get_avail_workers))),
    ?assertEqual(5, length(?sync(Pid, get_all_workers))),
    ?assertEqual(0, length(?sync(Pid, get_all_monitors))),
    ok = ?sync(Pid, stop).

checkin_after_exception_in_transaction() ->
    {ok, Pool} = new_pool(2, 0),
    ?assertEqual(2, length(?sync(Pool, get_avail_workers))),
    Tx = fun(Worker) ->
        ?assert(is_pid(Worker)),
        ?assertEqual(1, length(?sync(Pool, get_avail_workers))),
        throw(it_on_the_ground),
        ?assert(false)
    end,
    try
        poolboy:transaction(Pool, Tx)
    catch
        throw:it_on_the_ground -> ok
    end,
    ?assertEqual(2, length(?sync(Pool, get_avail_workers))),
    ok = ?sync(Pool, stop).

pool_returns_status() ->
    {ok, Pool} = new_pool(2, 0),
    ?assertEqual({ready, 2, 0, 0}, poolboy:status(Pool)),
    poolboy:checkout(Pool),
    ?assertEqual({ready, 1, 0, 1}, poolboy:status(Pool)),
    poolboy:checkout(Pool),
    ?assertEqual({full, 0, 0, 2}, poolboy:status(Pool)),
    ok = ?sync(Pool, stop),

    {ok, Pool2} = new_pool(1, 1),
    ?assertEqual({ready, 1, 0, 0}, poolboy:status(Pool2)),
    poolboy:checkout(Pool2),
    ?assertEqual({overflow, 0, 0, 1}, poolboy:status(Pool2)),
    poolboy:checkout(Pool2),
    ?assertEqual({full, 0, 1, 2}, poolboy:status(Pool2)),
    ok = ?sync(Pool2, stop),

    {ok, Pool3} = new_pool(0, 2),
    ?assertEqual({overflow, 0, 0, 0}, poolboy:status(Pool3)),
    poolboy:checkout(Pool3),
    ?assertEqual({overflow, 0, 1, 1}, poolboy:status(Pool3)),
    poolboy:checkout(Pool3),
    ?assertEqual({full, 0, 2, 2}, poolboy:status(Pool3)),
    ok = ?sync(Pool3, stop),

    {ok, Pool4} = new_pool(0, 0),
    ?assertEqual({full, 0, 0, 0}, poolboy:status(Pool4)),
    ok = ?sync(Pool4, stop).

new_pool(Size, MaxOverflow) ->
    poolboy:start_link([{name, {local, poolboy_test}},
                        {worker_module, poolboy_test_worker},
                        {size, Size}, {max_overflow, MaxOverflow}]).
