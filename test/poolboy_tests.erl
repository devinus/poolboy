-module(poolboy_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

pool_startup_test() ->
    %% check basic pool operation
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 10}, {max_overflow, 5}]),
    ?assertEqual(10, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    poolboy:checkout(Pid),
    ?assertEqual(9, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    Worker = poolboy:checkout(Pid),
    ?assertEqual(8, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    poolboy:checkin(Pid, Worker),
    ?assertEqual(9, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_overflow_test() ->
    %% check that the pool overflows properly
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 5}]),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(7, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    poolboy:checkin(Pid, A),
    poolboy:checkin(Pid, B),
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, C),
    poolboy:checkin(Pid, D),
    timer:sleep(500),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, E),
    poolboy:checkin(Pid, F),
    timer:sleep(500),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, G),
    timer:sleep(500),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_empty_test() ->
    %% checks the pool handles the empty condition correctly when overflow is
    %% enabled.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 2}]),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(7, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    Self = self(),
    spawn(fun() ->
                Worker = poolboy:checkout(Pid),
                Self ! got_worker,
                poolboy:checkin(Pid, Worker),
                timer:sleep(500)
        end),
    %% spawned process should block waiting for worker to be available
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    poolboy:checkin(Pid, A),
    poolboy:checkin(Pid, B),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, C),
    poolboy:checkin(Pid, D),
    timer:sleep(500),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, E),
    poolboy:checkin(Pid, F),
    timer:sleep(500),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, G),
    timer:sleep(500),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_empty_no_overflow_test() ->
    %% checks the pool handles the empty condition properly when overflow is
    %% disabled
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 0}]),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    [A, B, C, D, E] = Workers,
    Self = self(),
    spawn(fun() ->
                Worker = poolboy:checkout(Pid),
                Self ! got_worker,
                poolboy:checkin(Pid, Worker),
                timer:sleep(500)
        end),
    %% spawned process should block waiting for worker to be available
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    poolboy:checkin(Pid, A),
    poolboy:checkin(Pid, B),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    timer:sleep(500),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, C),
    poolboy:checkin(Pid, D),
    timer:sleep(500),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    poolboy:checkin(Pid, E),
    timer:sleep(500),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).


worker_death_test() ->
    %% this test checks that dead workers are only restarted when the pool is
    %% not full if the overflow count is 0. Meaning, don't restart overflow
    %% workers.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 2}]),
    Worker = poolboy:checkout(Pid),
    gen_server:call(Worker, die),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(7, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    gen_server:call(A, die),
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(6, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    gen_server:call(B, die),
    gen_server:call(C, die),
    timer:sleep(500),
    ?assertEqual(1, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

worker_death_while_full_test() ->
    %% this test checks that if a worker dies while the pool is full and there
    %% is a queued checkout, a new worker is started and the checkout
    %serviced. If there are no queued checkouts, a new worker is not started.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 2}]),
    Worker = poolboy:checkout(Pid),
    gen_server:call(Worker, die),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    [A, B|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(7, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    Self = self(),
    spawn(fun() ->
                poolboy:checkout(Pid),
                Self ! got_worker
                %% XXX don't release the worker. we want to also test
                %% what happens when the worker pool is full and a worker
                %% dies with no queued checkouts.
        end),
    %% spawned process should block waiting for worker to be available
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    gen_server:call(A, die),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    gen_server:call(B, die),
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(6, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).


worker_death_while_full_no_overflow_test() ->
    %% this test tests that if a worker dies while the pool is full AND
    %% there's no overflow, a new worker is started unconditionally and any
    %% queued checkouts are serviced
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 0}]),
    Worker = poolboy:checkout(Pid),
    gen_server:call(Worker, die),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    timer:sleep(500),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    Self = self(),
    spawn(fun() ->
                poolboy:checkout(Pid),
                Self ! got_worker
                %% XXX do not release, need to also test when worker dies
                %% and no checkouts queued
        end),
    %% spawned process should block waiting for worker to be available
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    gen_server:call(A, die),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    gen_server:call(B, die),
    timer:sleep(500),
    ?assertEqual(1, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    gen_server:call(C, die),
    timer:sleep(500),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),

    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

-endif.

