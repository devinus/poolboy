-module(poolboy_tests).

-include_lib("eunit/include/eunit.hrl").

pool_test_() ->
    {foreachx,
        fun(_) ->
            error_logger:tty(false)
        end,
        fun(_, _) ->
            case whereis(poolboy_test) of
                undefined -> ok;
                Pid -> pool_call(Pid, stop)
            end,
            error_logger:tty(true)
        end,
        [ {Type,
           fun(T, _) ->
                   {<<(atom_to_binary(T, latin1))/binary, <<": ">>/binary, Title/binary>>, fun() -> Test(T) end}
           end} || Type <- [list, array, tuple], {Title, Test} <-
        [
            {<<"Basic pool operations">>,
                fun pool_startup/1
            },
            {<<"Pool overflow should work">>,
                fun pool_overflow/1
            },
            {<<"Pool behaves when empty">>,
                fun pool_empty/1
            },
            {<<"Pool behaves when empty and oveflow is disabled">>,
                fun pool_empty_no_overflow/1
            },
            {<<"Pool behaves on worker death">>,
                fun worker_death/1
            },
            {<<"Pool behaves when full and a worker dies">>,
                fun worker_death_while_full/1
            },
            {<<"Pool behaves when full, a worker dies and overflow disabled">>,
                fun worker_death_while_full_no_overflow/1
            },
            {<<"Non-blocking pool behaves when full and overflow disabled">>,
                fun pool_full_nonblocking_no_overflow/1
            },
            {<<"Non-blocking pool behaves when full">>,
                fun pool_full_nonblocking/1
            },
            {<<"Pool behaves on owner death">>,
                fun owner_death/1
            },
            {<<"Worker checked-in after an exception in a transaction">>,
                fun checkin_after_exception_in_transaction/1
            },
            {<<"Pool returns status">>,
                fun pool_returns_status/1
            },
            {<<"Pool demonitors previously waiting processes">>,
                fun demonitors_previously_waiting_processes/1
            },
            {<<"Pool demonitors when a checkout is cancelled">>,
                fun demonitors_when_checkout_cancelled/1
            },
            {<<"Check that LIFO is the default strategy">>,
                fun default_strategy_lifo/1
            },
            {<<"Check LIFO strategy">>,
                fun lifo_strategy/1
            },
            {<<"Check FIFO strategy">>,
                fun fifo_strategy/1
            },
            {<<"Pool reuses waiting monitor when a worker exits">>,
                fun reuses_waiting_monitor_on_worker_exit/1
            },
            {<<"Recover from timeout without exit handling">>,
                fun transaction_timeout_without_exit/1
            },
            {<<"Recover from transaction timeout">>,
                fun transaction_timeout/1
            }
        ]
        ]
    }.

%% Tell a worker to exit and await its impending doom.
kill_worker(Pid) ->
    erlang:monitor(process, Pid),
    pool_call(Pid, die),
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


transaction_timeout_without_exit(Type) ->
    {ok, Pid} = new_pool(1, 0, lifo, Type),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)),
    WorkerList = pool_call(Pid, get_all_workers),
    ?assertMatch([_], WorkerList),
    spawn(poolboy, transaction, [Pid,
        fun(Worker) ->
            ok = pool_call(Worker, work)
        end,
        0]),
    timer:sleep(100),
    ?assertEqual(WorkerList, pool_call(Pid, get_all_workers)),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)).


transaction_timeout(Type) ->
    {ok, Pid} = new_pool(1, 0, lifo, Type),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)),
    WorkerList = pool_call(Pid, get_all_workers),
    ?assertMatch([_], WorkerList),
    ?assertExit(
        {timeout, _},
        poolboy:transaction(Pid,
            fun(Worker) ->
                ok = pool_call(Worker, work)
            end,
            0)),
    ?assertEqual(WorkerList, pool_call(Pid, get_all_workers)),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)).


pool_startup(Type) ->
    %% Check basic pool operation.
    {ok, Pid} = new_pool(10, 5, lifo, Type),
    ?assertEqual(10, length(pool_call(Pid, get_avail_workers))),
    poolboy:checkout(Pid),
    ?assertEqual(9, length(pool_call(Pid, get_avail_workers))),
    Worker = poolboy:checkout(Pid),
    ?assertEqual(8, length(pool_call(Pid, get_avail_workers))),
    checkin_worker(Pid, Worker),
    ?assertEqual(9, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(1, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_overflow(Type) ->
    %% Check that the pool overflows properly.
    {ok, Pid} = new_pool(5, 5, lifo, Type),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_empty(Type) ->
    %% Checks that the the pool handles the empty condition correctly when
    %% overflow is enabled.
    {ok, Pid} = new_pool(5, 2, lifo, Type),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
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
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_empty_no_overflow(Type) ->
    %% Checks the pool handles the empty condition properly when overflow is
    %% disabled.
    {ok, Pid} = new_pool(5, 0, lifo, Type),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
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
    ?assertEqual(2, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(4, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

worker_death(Type) ->
    %% Check that dead workers are only restarted when the pool is not full
    %% and the overflow count is 0. Meaning, don't restart overflow workers.
    {ok, Pid} = new_pool(5, 2, lifo, Type),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    kill_worker(A),
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    kill_worker(B),
    kill_worker(C),
    ?assertEqual(1, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(4, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

worker_death_while_full(Type) ->
    %% Check that if a worker dies while the pool is full and there is a
    %% queued checkout, a new worker is started and the checkout serviced.
    %% If there are no queued checkouts, a new worker is not started.
    {ok, Pid} = new_pool(5, 2, lifo, Type),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    [A, B|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
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
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

worker_death_while_full_no_overflow(Type) ->
    %% Check that if a worker dies while the pool is full and there's no
    %% overflow, a new worker is started unconditionally and any queued
    %% checkouts are serviced.
    {ok, Pid} = new_pool(5, 0, lifo, Type),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
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
    ?assertEqual(1, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    kill_worker(C),
    ?assertEqual(2, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(3, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_full_nonblocking_no_overflow(Type) ->
    %% Check that when the pool is full, checkouts return 'full' when the
    %% option to use non-blocking checkouts is used.
    {ok, Pid} = new_pool(5, 0, lifo, Type),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    ?assertEqual(A, poolboy:checkout(Pid)),
    ?assertEqual(5, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_full_nonblocking(Type) ->
    %% Check that when the pool is full, checkouts return 'full' when the
    %% option to use non-blocking checkouts is used.
    {ok, Pid} = new_pool(5, 5, lifo, Type),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 9)],
    ?assertEqual(0, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(10, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    NewWorker = poolboy:checkout(Pid, false),
    ?assertEqual(false, is_process_alive(A)), %% Overflow workers get shutdown
    ?assert(is_pid(NewWorker)),
    ?assertEqual(full, poolboy:checkout(Pid, false)),
    ?assertEqual(10, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

owner_death(Type) ->
    %% Check that a dead owner (a process that dies with a worker checked out)
    %% causes the pool to dismiss the worker and prune the state space.
    {ok, Pid} = new_pool(5, 5, lifo, Type),
    spawn(fun() ->
        poolboy:checkout(Pid),
        receive after 500 -> exit(normal) end
    end),
    timer:sleep(1000),
    ?assertEqual(5, length(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

checkin_after_exception_in_transaction(Type) ->
    {ok, Pool} = new_pool(2, 0, lifo, Type),
    ?assertEqual(2, length(pool_call(Pool, get_avail_workers))),
    Tx = fun(Worker) ->
        ?assert(is_pid(Worker)),
        ?assertEqual(1, length(pool_call(Pool, get_avail_workers))),
        throw(it_on_the_ground),
        ?assert(false)
    end,
    try
        poolboy:transaction(Pool, Tx)
    catch
        throw:it_on_the_ground -> ok
    end,
    ?assertEqual(2, length(pool_call(Pool, get_avail_workers))),
    ok = pool_call(Pool, stop).

pool_returns_status(Type) ->
    {ok, Pool} = new_pool(2, 0, lifo, Type),
    ?assertEqual({ready, 2, 0, 0}, poolboy:status(Pool)),
    poolboy:checkout(Pool),
    ?assertEqual({ready, 1, 0, 1}, poolboy:status(Pool)),
    poolboy:checkout(Pool),
    ?assertEqual({full, 0, 0, 2}, poolboy:status(Pool)),
    ok = pool_call(Pool, stop),

    {ok, Pool2} = new_pool(1, 1),
    ?assertEqual({ready, 1, 0, 0}, poolboy:status(Pool2)),
    poolboy:checkout(Pool2),
    ?assertEqual({overflow, 0, 0, 1}, poolboy:status(Pool2)),
    poolboy:checkout(Pool2),
    ?assertEqual({full, 0, 1, 2}, poolboy:status(Pool2)),
    ok = pool_call(Pool2, stop),

    {ok, Pool3} = new_pool(0, 2),
    ?assertEqual({overflow, 0, 0, 0}, poolboy:status(Pool3)),
    poolboy:checkout(Pool3),
    ?assertEqual({overflow, 0, 1, 1}, poolboy:status(Pool3)),
    poolboy:checkout(Pool3),
    ?assertEqual({full, 0, 2, 2}, poolboy:status(Pool3)),
    ok = pool_call(Pool3, stop),

    {ok, Pool4} = new_pool(0, 0),
    ?assertEqual({full, 0, 0, 0}, poolboy:status(Pool4)),
    ok = pool_call(Pool4, stop).

demonitors_previously_waiting_processes(Type) ->
    {ok, Pool} = new_pool(1,0, lifo, Type),
    Self = self(),
    Pid = spawn(fun() ->
        W = poolboy:checkout(Pool),
        Self ! ok,
        timer:sleep(500),
        poolboy:checkin(Pool, W),
        receive ok -> ok end
    end),
    receive ok -> ok end,
    Worker = poolboy:checkout(Pool),
    ?assertEqual(1, length(get_monitors(Pool))),
    poolboy:checkin(Pool, Worker),
    timer:sleep(500),
    ?assertEqual(0, length(get_monitors(Pool))),
    Pid ! ok,
    ok = pool_call(Pool, stop).

demonitors_when_checkout_cancelled(Type) ->
    {ok, Pool} = new_pool(1,0, lifo, Type),
    Self = self(),
    Pid = spawn(fun() ->
        poolboy:checkout(Pool),
        _ = (catch poolboy:checkout(Pool, true, 1000)),
        Self ! ok,
        receive ok -> ok end
    end),
    timer:sleep(500),
    ?assertEqual(2, length(get_monitors(Pool))),
    receive ok -> ok end,
    ?assertEqual(1, length(get_monitors(Pool))),
    Pid ! ok,
    ok = pool_call(Pool, stop).

default_strategy_lifo(Type) ->
    %% Default strategy is LIFO
    {ok, Pid} = new_pool(2, 0, default, Type),
    Worker1 = poolboy:checkout(Pid),
    ok = poolboy:checkin(Pid, Worker1),
    Worker1 = poolboy:checkout(Pid),
    poolboy:stop(Pid).

lifo_strategy(Type) ->
    {ok, Pid} = new_pool(2, 0, lifo, Type),
    Worker1 = poolboy:checkout(Pid),
    ok = poolboy:checkin(Pid, Worker1),
    Worker1 = poolboy:checkout(Pid),
    poolboy:stop(Pid).

fifo_strategy(Type) ->
    {ok, Pid} = new_pool(2, 0, fifo, Type),
    Worker1 = poolboy:checkout(Pid),
    ok = poolboy:checkin(Pid, Worker1),
    Worker2 = poolboy:checkout(Pid),
    ?assert(Worker1 =/= Worker2),
    Worker1 = poolboy:checkout(Pid),
    poolboy:stop(Pid).

reuses_waiting_monitor_on_worker_exit(Type) ->
    {ok, Pool} = new_pool(1,0, lifo, Type),

    Self = self(),
    Pid = spawn(fun() ->
        Worker = poolboy:checkout(Pool),
        Self ! {worker, Worker},
        poolboy:checkout(Pool),
        receive ok -> ok end
    end),

    Worker = receive {worker, Worker1} -> Worker1 end,
    Ref = monitor(process, Worker),
    exit(Worker, kill),
    receive
        {'DOWN', Ref, _, _, _} ->
            ok
    end,

    ?assertEqual(1, length(get_monitors(Pool))),

    Pid ! ok,
    ok = pool_call(Pool, stop).

get_monitors(Pid) ->
    %% Synchronise with the Pid to ensure it has handled all expected work.
    _ = sys:get_status(Pid),
    [{monitors, Monitors}] = erlang:process_info(Pid, [monitors]),
    Monitors.

new_pool(Size, MaxOverflow) ->
    poolboy:start_link([{name, {local, poolboy_test}},
                        {worker_module, poolboy_test_worker},
                        {size, Size}, {max_overflow, MaxOverflow}]).

new_pool(Size, MaxOverflow, default, Type) ->
    poolboy:start_link([{name, {local, poolboy_test}},
                        {worker_module, poolboy_test_worker},
                        {size, Size}, {max_overflow, MaxOverflow}, {type, Type}]);

new_pool(Size, MaxOverflow, Strategy, Type) ->
    poolboy:start_link([{name, {local, poolboy_test}},
                        {worker_module, poolboy_test_worker},
                        {size, Size}, {max_overflow, MaxOverflow}, {type, Type},
                        {strategy, Strategy}]).

pool_call(ServerRef, stop) when is_pid(ServerRef) ->
    case is_process_alive(ServerRef) of
        true -> gen_server:stop(ServerRef);
        _ -> ok
    end;
pool_call(ServerRef, Request) ->
    gen_server:call(ServerRef, Request).
