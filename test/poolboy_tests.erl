-module(poolboy_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

pool_test_() ->
    {foreach,
        fun() ->
                error_logger:tty(false)
        end,
        fun(_) ->
                case whereis(poolboy_test) of
                    undefined -> ok;
                    Pid -> gen_fsm:sync_send_all_state_event(Pid, stop)
                end,
                error_logger:tty(true)
        end,
        [
            {"Basic pool operations",
                fun pool_startup/0
            },
            {"Pool overflow should work",
                fun pool_overflow/0
            },
            {"Pool behaves right when it's empty",
                fun pool_empty/0
            },
            {"Pool behaves right when it's empty and oveflow is disabled",
                fun pool_empty_no_overflow/0
            },
            {"Pool behaves right on worker death",
                fun worker_death/0
            },
            {"Pool behaves right when it's full and a worker dies",
                fun worker_death_while_full/0
            },
            {"Pool behaves right when it's full, a worker dies and overflow is disabled",
                fun worker_death_while_full_no_overflow/0
            },
            {"Non-blocking pool behaves when it's full and overflow is disabled",
                fun pool_full_nonblocking_no_overflow/0
            },
            {"Non-blocking pool behaves when it's full",
                fun pool_full_nonblocking/0
            },
            {"Pool behaves right on user death",
                fun user_death/0
            }
        ]
    }.


%% tell a worker to exit and await its impending doom
kill_worker(Pid) ->
    erlang:monitor(process, Pid),
    gen_server:call(Pid, die),
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    end.

checkin_worker(Pid, Worker) ->
    %% there's no easy way to wait for a checkin to complete, because it's
    %% async and the supervisor may kill the process if it was an overflow
    %% worker. Yhe only solution seems to be a nasty hardcoded sleep.
    poolboy:checkin(Pid, Worker),
    timer:sleep(500).

pool_startup() ->
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
    checkin_worker(Pid, Worker),
    ?assertEqual(9, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(1, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_overflow() ->
    %% check that the pool overflows properly
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 5}]),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(7, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_empty() ->
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
                checkin_worker(Pid, Worker)
        end),
    %% spawned process should block waiting for worker to be available
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_empty_no_overflow() ->
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
                checkin_worker(Pid, Worker)
        end),
    %% spawned process should block waiting for worker to be available
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    checkin_worker(Pid, E),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).


worker_death() ->
    %% this test checks that dead workers are only restarted when the pool is
    %% not full if the overflow count is 0. Meaning, don't restart overflow
    %% workers.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 2}]),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(7, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    kill_worker(A),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(6, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    kill_worker(B),
    kill_worker(C),
    ?assertEqual(1, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(4, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

worker_death_while_full() ->
    %% this test checks that if a worker dies while the pool is full and there
    %% is a queued checkout, a new worker is started and the checkout
    %serviced. If there are no queued checkouts, a new worker is not started.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 2}]),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    [A, B|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
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
    kill_worker(A),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    kill_worker(B),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(6, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(6, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).


worker_death_while_full_no_overflow() ->
    %% this test tests that if a worker dies while the pool is full AND
    %% there's no overflow, a new worker is started unconditionally and any
    %% queued checkouts are serviced
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}}, {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 0}]),
    Worker = poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    [A, B, C|_Workers] = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
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
    kill_worker(A),
    %% spawned process should have been able to obtain a worker
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    kill_worker(B),
    ?assertEqual(1, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    kill_worker(C),
    ?assertEqual(2, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(3, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),

    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_full_nonblocking_no_overflow() ->
    %% check that when the pool is full, checkouts return 'full' when the
    %% option to checkouts nonblocking is enabled.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}},
            {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 0}, {checkout_blocks, false}]),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(full, poolboy:checkout(Pid)),
    ?assertEqual(full, poolboy:checkout(Pid)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    ?assertEqual(A, poolboy:checkout(Pid)),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

pool_full_nonblocking() ->
    %% check that when the pool is full, checkouts return 'full' when the
    %% option to checkouts nonblocking is enabled.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}},
            {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 5}, {checkout_blocks, false}]),
    Workers = [poolboy:checkout(Pid) || _ <- lists:seq(0, 9)],
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(10, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(full, poolboy:checkout(Pid)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    NewWorker = poolboy:checkout(Pid),
    ?assertEqual(false, is_process_alive(A)), %% overflow workers get shut down
    ?assert(is_pid(NewWorker)),
    ?assertEqual(full, poolboy:checkout(Pid)),
    ?assertEqual(10, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).

user_death() ->
    %% check that a dead user (a process that died with a worker checked out)
    %% causes the pool to dismiss the worker and prune the state space.
    {ok, Pid} = poolboy:start_link([{name, {local, poolboy_test}},
            {worker_module, poolboy_test_worker},
            {size, 5}, {max_overflow, 5}, {checkout_blocks, false}]),
    spawn(fun() ->
                  %% you'll have to pry it from my cold, dead hands
                  poolboy:checkout(Pid),
                  receive after 500 -> exit(normal) end
          end),
    %% on a long enough timeline, the survival rate for everyone drops to zero.
    receive after 1000 -> ok end,
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_avail_workers))),
    ?assertEqual(5, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_workers))),
    ?assertEqual(0, length(gen_fsm:sync_send_all_state_event(Pid,
                get_all_monitors))),
    ok = gen_fsm:sync_send_all_state_event(Pid, stop).
    

-endif.

