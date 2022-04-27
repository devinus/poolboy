-module(prop_poolboy).
-include_lib("proper/include/proper.hrl").

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_startup
%% @doc Basic pool operations
prop_pool_startup() ->
    ?FORALL({Size,MaxOverflow}, ?SUCHTHAT({Size, MaxOverflow}, {range(2,100), range(1,100)}, Size / 2 >= MaxOverflow),
        begin
			{ok, Pool} = new_pool(Size, MaxOverflow),
			
            %% Check basic pool operation.
			WorkerCount = queue:len(pool_call(Pool, get_avail_workers)),
			WorkerCount = Size,
			poolboy:checkout(Pool),
			WorkerCount_1 = queue:len(pool_call(Pool, get_avail_workers)),
			WorkerCount_1 = WorkerCount - 1,
			Worker = poolboy:checkout(Pool),
			WorkerCount_2 = queue:len(pool_call(Pool, get_avail_workers)),
			WorkerCount_2 = WorkerCount - 2,
			checkin_worker(Pool, Worker),
			WorkerCount_1 = queue:len(pool_call(Pool, get_avail_workers)),
			
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			MonitorCount = 1, 
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_overflow
%% @doc Pool overflow should work
prop_pool_overflow() ->
	 ?FORALL({Size,MaxOverflow}, ?SUCHTHAT({Size, MaxOverflow}, {range(8,100), range(8,100)}, Size / 2 >= MaxOverflow),
	%?FORALL(_Bool, boolean(),
		begin
			%% Check that the pool overflows properly.
			{ok, Pool} = new_pool(Size, MaxOverflow),
			% make a list of workers
			Workers = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size+1)],
			Avail_workers = pool_call(Pool, get_avail_workers),
			All_workers = pool_call(Pool, get_all_workers),
			Avail_workersLength = queue:len(Avail_workers),
			All_workersLength = length(All_workers),
						
			% io:format("Avail_workers=~p~n",[pool_call(Pool, get_avail_workers)]),
			% io:format("All_workers=~p~n",[pool_call(Pool, get_all_workers)]),
			% io:format("All_monitors=~p~n",[pool_call(Pool, get_all_monitors)]),
			% io:format("Avail_workers length=~p~n",[queue:len(pool_call(Pool, get_avail_workers))]),
			% io:format("All_workers length=~p~n",[length(pool_call(Pool, get_all_workers))]),
			% io:format("All_monitors length=~p~n~n",[length(pool_call(Pool, get_all_monitors))]),						
			
			Avail_workersLength = 0,
			All_workersLength = Size+2,
			
			[A, B, C, D, E, F, G|_Rest] = Workers,
			
			checkin_worker(Pool, A),
			checkin_worker(Pool, B),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) + 0,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			% io:format("=> Check in A, Check in B~n",[]),
			% io:format("Avail_workers=~p~n",[pool_call(Pool, get_avail_workers)]),
			% io:format("All_workers=~p~n",[pool_call(Pool, get_all_workers)]),
			% io:format("Avail_workers length=~p~n",[queue:len(pool_call(Pool, get_avail_workers))]),
			% io:format("All_workers length=~p~n",[length(pool_call(Pool, get_all_workers))]),
			% io:format("All_monitors length=~p~n~n",[length(pool_call(Pool, get_all_monitors))]),
			
			checkin_worker(Pool, C),
			checkin_worker(Pool, D),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 2,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			% io:format("=> Check in C, Check in D~n",[]),
			% io:format("Avail_workers=~p~n",[pool_call(Pool, get_avail_workers)]),
			% io:format("All_workers=~p~n",[pool_call(Pool, get_all_workers)]),
			% io:format("Avail_workers length=~p~n",[queue:len(pool_call(Pool, get_avail_workers))]),
			% io:format("All_workers length=~p~n",[length(pool_call(Pool, get_all_workers))]),
			% io:format("All_monitors length=~p~n~n",[length(pool_call(Pool, get_all_monitors))]),
			
			checkin_worker(Pool, E),
			checkin_worker(Pool, F),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 4,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			% io:format("=> Check in E, Check in F~n",[]),
			% io:format("Avail_workers=~p~n",[pool_call(Pool, get_avail_workers)]),
			% io:format("All_workers=~p~n",[pool_call(Pool, get_all_workers)]),
			% io:format("Avail_workers length=~p~n",[queue:len(pool_call(Pool, get_avail_workers))]),
			% io:format("All_workers length=~p~n",[length(pool_call(Pool, get_all_workers))]),
			% io:format("All_monitors length=~p~n~n",[length(pool_call(Pool, get_all_monitors))]),
			
			checkin_worker(Pool, G),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 5,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			% io:format("=> Check in G~n",[]),
			% io:format("Avail_workers=~p~n",[pool_call(Pool, get_avail_workers)]),
			% io:format("All_workers=~p~n",[pool_call(Pool, get_all_workers)]),
			% io:format("Avail_workers length=~p~n",[queue:len(pool_call(Pool, get_avail_workers))]),
			% io:format("All_workers length=~p~n",[length(pool_call(Pool, get_all_workers))]),
			% io:format("All_monitors length=~p~n~n",[length(pool_call(Pool, get_all_monitors))]),
			
			ok == poolboy:stop(Pool)
		end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_empty_no_overflow
%% @doc Pool behaves when empty and oveflow is disabled
prop_pool_empty_no_overflow() ->
	 ?FORALL(Size, range(5,100),
		begin
			%% Checks the pool handles the empty condition properly when overflow is
			%% disabled.
			{ok, Pool} = new_pool(Size, 0),
			Workers = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size-1)],
			Avail_workers = pool_call(Pool, get_avail_workers),
			All_workers = pool_call(Pool, get_all_workers),
			Avail_workersLength = queue:len(Avail_workers),
			All_workersLength = length(All_workers),
			
			Avail_workersLength = 0,
			All_workersLength = Size,
			
			[A, B, C, D, E | _Rest] = Workers,
			Self = self(),
			spawn(fun() ->
				Worker = poolboy:checkout(Pool),
				Self ! got_worker,
				checkin_worker(Pool, Worker)
			end),

			%% Spawned process should block waiting for worker to be available.
			BlockWaitingResult = true,
			receive
				got_worker -> 
					case BlockWaitingResult == false of
						true ->
							throw(badarg);
						false -> ok
					end
			after
				500 -> BlockWaitingResult = true
			end,
			
			checkin_worker(Pool, A),
			checkin_worker(Pool, B),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 2,
			All_workersLength = length(pool_call(Pool, get_all_workers)),

			%% Spawned process should have been able to obtain a worker.
			receive
				got_worker -> 
					case BlockWaitingResult == true of
						false ->
							throw(badarg);
						true -> ok
					end
			after
				500 -> throw(badarg)
			end,

			checkin_worker(Pool, C),
			checkin_worker(Pool, D),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 4,
			All_workersLength = length(pool_call(Pool, get_all_workers)),
			
			checkin_worker(Pool, E),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 5,
			All_workersLength = length(pool_call(Pool, get_all_workers)),
			
			ok == poolboy:stop(Pool)
		end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_empty
%% @doc Pool behaves when empty
prop_pool_empty() ->
	 ?FORALL({Size,MaxOverflow}, ?SUCHTHAT({Size, MaxOverflow}, {range(8,100), range(8,100)}, Size / 2 >= MaxOverflow),
	%?FORALL(_Bool, boolean(),
		begin
			%% Check that the pool overflows properly.
			{ok, Pool} = new_pool(Size, MaxOverflow),
			
			Workers = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size+1)],
			Avail_workers = pool_call(Pool, get_avail_workers),
			All_workers = pool_call(Pool, get_all_workers),
			Avail_workersLength = queue:len(Avail_workers),
			All_workersLength = length(All_workers),
		
			Avail_workersLength = 0,
			All_workersLength = Size+2,
			
			
			[A, B, C, D, E, F, G|_Rest] = Workers,
			
		    Self = self(),
			spawn(fun() ->
				Worker = poolboy:checkout(Pool),
				Self ! got_worker,
				checkin_worker(Pool, Worker)
			end),

			%% Spawned process should block waiting for worker to be available.
			BlockWaitingResult = true,
			receive
				got_worker -> 
					case BlockWaitingResult == false of
						true ->
							throw(badarg);
						false -> ok
					end
			after
				500 -> BlockWaitingResult = true
			end,
			
			checkin_worker(Pool, A),
			checkin_worker(Pool, B),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) + 0,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,

			spawn(fun() ->
				Worker = poolboy:checkout(Pool),
				Self ! got_worker,
				checkin_worker(Pool, Worker)
			end),
			
			%% Spawned process should have been able to obtain a worker.
			receive
				got_worker -> 
					case BlockWaitingResult == true of
						false ->
							throw(badarg);
						true -> ok
					end
			after
				500 -> throw(badarg)
			end,
			
			checkin_worker(Pool, C),
			checkin_worker(Pool, D),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 2,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			checkin_worker(Pool, E),
			checkin_worker(Pool, F),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 4,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			checkin_worker(Pool, G),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - 5,
			All_workersLength = length(pool_call(Pool, get_all_workers)) + 2,
			
			ok == poolboy:stop(Pool)
		end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_worker_death
%% @doc Pool behaves on worker death
prop_pool_worker_death() ->				
    ?FORALL({Size,MaxOverflow}, ?SUCHTHAT({Size, MaxOverflow}, {range(2,100), range(1,100)}, Size / 2 >= MaxOverflow),
	%?FORALL(_Bool, boolean(),
        begin
			%% Check that dead workers are only restarted when the pool is not full
			%% and the overflow count is 0. Meaning, don't restart overflow workers.
			{ok, Pool} = new_pool(Size, MaxOverflow),
			
			Worker = poolboy:checkout(Pool),
			kill_worker(Worker),
			
			All_workers = pool_call(Pool, get_all_workers),
			All_workersLength = length(All_workers),
			
			All_workersLength = Size,
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - Size,
    		[A, B, C|_Workers] = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size+1)],
			Avail_workers = pool_call(Pool, get_avail_workers),
			Avail_workersLength = queue:len(Avail_workers),
			Avail_workersLength = 0,
			All_workersLength = length(pool_call(Pool, get_all_workers))-2,
			
			kill_worker(A),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),
			All_workersLength = length(pool_call(Pool, get_all_workers))-1,
			kill_worker(B),
			kill_worker(C),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers))-1,
			All_workersLength = length(pool_call(Pool, get_all_workers)),
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			MonitorCount = Size - 1, 
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_worker_death_while_full
%% @doc Pool behaves when full and a worker dies
prop_worker_death_while_full() ->
    ?FORALL({Size,MaxOverflow}, ?SUCHTHAT({Size, MaxOverflow}, {range(2,100), range(1,100)}, Size / 2 >= MaxOverflow),
        begin
			%% Check that if a worker dies while the pool is full and there is a
			%% queued checkout, a new worker is started and the checkout serviced.
			%% If there are no queued checkouts, a new worker is not started.
    		{ok, Pool} = new_pool(Size, MaxOverflow),
			Worker = poolboy:checkout(Pool),
			kill_worker(Worker),	

			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - Size,
			All_workers = pool_call(Pool, get_all_workers),
			All_workersLength = length(All_workers),
			All_workersLength = Size,
			
			[A, B|_Workers] = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size+1)],
			Avail_workers = pool_call(Pool, get_avail_workers),
			Avail_workersLength = queue:len(Avail_workers),
			Avail_workersLength = 0,
			All_workersLength = length(pool_call(Pool, get_all_workers))-2,
			
			Self = self(),
			spawn(fun() ->
				poolboy:checkout(Pool),
				Self ! got_worker,
				%% XXX: Don't release the worker. We want to also test what happens
				%% when the worker pool is full and a worker dies with no queued
				%% checkouts.
				timer:sleep(5000)
			end),

			%% Spawned process should block waiting for worker to be available.
			BlockWaitingResult = true,

			kill_worker(A),
			
			%% Spawned process should have been able to obtain a worker.
			receive
				got_worker -> 
					case BlockWaitingResult == true of
						false ->
							throw(badarg);
						true -> ok
					end
			after
				1000 -> throw(badarg)
			end,
			
			kill_worker(B),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),
			%io:format("All_workersLength = ~p, Size=~p~n",[length(pool_call(Pool, get_all_workers)),Size]),
			All_workersLength = length(pool_call(Pool, get_all_workers))-1,
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			MonitorCount = Size + 1, 
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_worker_death_while_full_no_overflow
%% @doc Pool behaves when full, a worker dies and overflow disabled
prop_worker_death_while_full_no_overflow() ->
    ?FORALL(Size, range(5,100),
        begin
			%% Check that if a worker dies while the pool is full and there's no
			%% overflow, a new worker is started unconditionally and any queued
			%% checkouts are serviced.
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
    		Worker = poolboy:checkout(Pool),
			kill_worker(Worker),	
			
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) - Size,
			All_workers = pool_call(Pool, get_all_workers),
			All_workersLength = length(All_workers),
			All_workersLength = Size,
			
			[A, B, C|_Workers] = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size - 1)],
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),
			All_workersLength = length(pool_call(Pool, get_all_workers)),
			
			Self = self(),
			spawn(fun() ->
				poolboy:checkout(Pool),
				Self ! got_worker,
				%% XXX: Do not release, need to also test when worker dies and no
				%% checkouts queued.
				timer:sleep(5000)
			end),

			%% Spawned process should block waiting for worker to be available.
			BlockWaitingResult = true,
			receive
				got_worker -> 
					case BlockWaitingResult == false of
						true ->
							throw(badarg);
						false -> ok
					end
			after
				500 -> BlockWaitingResult = true
			end,
			
			kill_worker(A),
			
			%% Spawned process should have been able to obtain a worker.
			receive
				got_worker -> 
					case BlockWaitingResult == true of
						false ->
							throw(badarg);
						true -> ok
					end
			after
				1000 -> throw(badarg)
			end,
			
			kill_worker(B),
			
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers))-1,
			All_workersLength = length(pool_call(Pool, get_all_workers)),
			
			kill_worker(C),
			
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers))-2,
			All_workersLength = length(pool_call(Pool, get_all_workers)),
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			MonitorCount = Size - 2, 
			
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_full_nonblocking_no_overflow
%% @doc Non-blocking pool behaves when full and overflow disabled
prop_pool_full_nonblocking_no_overflow()->
    ?FORALL(Size, range(5,100),
        begin
			%% Check that when the pool is full, checkouts return 'full' when the
			%% option to use non-blocking checkouts is used.
    		MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
    		Workers = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size - 1)],
	
			Avail_workersLength = 0,
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),	
			Size = length(pool_call(Pool, get_all_workers)),
			full = poolboy:checkout(Pool, false),
			full = poolboy:checkout(Pool, false),
			
			A = hd(Workers),
			checkin_worker(Pool, A),
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			%io:format("MonitorCount=~p, Size=~p~n",[MonitorCount, Size]),
			MonitorCount = Size-1, 
	
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_full_nonblocking
%% @doc Non-blocking pool behaves when full
prop_pool_full_nonblocking()->
    ?FORALL(Size, range(5,5),
        begin
			%% Check that when the pool is full, checkouts return 'full' when the
			%% option to use non-blocking checkouts is used.
			MaxOverflow = Size,
    		{ok, Pool} = new_pool(Size, MaxOverflow),
    
			Workers = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size+4)],
			Avail_workersLength = 0,
			All_workersLength = Size,
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),	
			All_workersLength = length(pool_call(Pool, get_all_workers))-5,
			%io:format("All_workersLength =~p, Size=~p~n",[length(pool_call(Pool, get_all_workers)),Size]),
			full = poolboy:checkout(Pool, false),
			%io:format("State =~p~n",[poolboy:checkout(Pool, false)]),
			A = hd(Workers),
			checkin_worker(Pool, A),
			NewWorker = poolboy:checkout(Pool, false),
			
			%% Overflow workers get shutdown
			false = is_process_alive(A), 
			true = is_pid(NewWorker),
			full = poolboy:checkout(Pool, false),
			%io:format("State =~p~n",[poolboy:checkout(Pool, false)]),
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			MonitorCount = Size+5, 
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_owner_death
%% @doc Pool behaves on owner death
prop_owner_death()->
    ?FORALL(Size, range(5,100),
        begin
			%% Check that a dead owner (a process that dies with a worker checked out)
			%% causes the pool to dismiss the worker and prune the state space.
			MaxOverflow = Size,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			spawn(fun() ->
				poolboy:checkout(Pool),
				receive after 500 -> exit(normal) end
			end),
			timer:sleep(1000),
			
			Size = queue:len(pool_call(Pool, get_avail_workers)),
			Size = length(pool_call(Pool, get_all_workers)),
			MonitorCount = length(pool_call(Pool, get_all_monitors)),
			MonitorCount = 0, 
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_checkin_after_exception_in_transaction
%% @doc Worker checked-in after an exception in a transaction
prop_checkin_after_exception_in_transaction() ->
    ?FORALL(Size, range(5,100),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),		
			Tx = fun(Worker) ->
				true = is_pid(Worker),
				Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)) + 1,
				throw(it_on_the_ground)
			end,
		
			try
				poolboy:transaction(Pool, Tx)
			catch
				throw:it_on_the_ground -> ok
			end,
			Avail_workersLength = queue:len(pool_call(Pool, get_avail_workers)),
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_pool_returns_status
%% @doc Pool returns status
prop_pool_returns_status() ->
	?FORALL(Size, range(5,100),
        begin
			fun() ->
				MaxOverflow = 0,
				{ok, Pool} = new_pool(Size, MaxOverflow),
				fun() ->
					StateName = ready, 
					QueueLength = Size, 
					Overflow = 0, 
					CurrentSize = 0,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),

				poolboy:checkout(Pool),

				fun() ->
					StateName = ready, 
					QueueLength = Size - 1, 
					Overflow = 0, 
					CurrentSize = 1,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),

				poolboy:checkout(Pool),

				fun() ->
					StateName = ready, 
					QueueLength = Size - 2, 
					Overflow = 0, 
					CurrentSize = 2,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),
				
				ok = poolboy:stop(Pool)
			end(),
			
			fun() ->
				MaxOverflow = Size,
				{ok, Pool} = new_pool(Size, MaxOverflow),
				fun() ->
					StateName = ready, 
					QueueLength = Size, 
					Overflow = 0, 
					CurrentSize = 0,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),

				poolboy:checkout(Pool),

				fun() ->
					StateName = ready, 
					QueueLength = Size - 1, 
					Overflow = 0, 
					CurrentSize = 1,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),

				poolboy:checkout(Pool),

				fun() ->
					StateName = ready, 
					QueueLength = Size - 2, 
					Overflow = 0, 
					CurrentSize = 2,
					{StateName, QueueLength, Overflow, CurrentSize} = poolboy:status(Pool)
				end(),
				
				ok = poolboy:stop(Pool)
			end(),
			
			fun() ->
				MaxOverflow = Size,
				{ok, Pool} = new_pool(0, MaxOverflow),
				fun() ->
					StateName = overflow, 
					QueueLength = 0, 
					Overflow = 0, 
					CurrentSize = 0,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),
				
				poolboy:checkout(Pool),
				
				fun() ->
					StateName = overflow, 
					QueueLength = 0, 
					Overflow = 1, 
					CurrentSize = 1,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),
				
				poolboy:checkout(Pool),
				
				fun() ->
					StateName = overflow, 
					QueueLength = 0, 
					Overflow = 2, 
					CurrentSize = 2,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),
				
				ok = poolboy:stop(Pool)
			end(),

			fun() ->
				{ok, Pool} = new_pool(0, 0),
				fun() ->
					StateName = full, 
					QueueLength = 0, 
					Overflow = 0, 
					CurrentSize = 0,
					{StateName, QueueLength, Overflow, CurrentSize} = 
						 poolboy:status(Pool)
				end(),
				
				ok = poolboy:stop(Pool)
			end(),			

			true
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_demonitors_previously_waiting_processes
%% @doc Pool demonitors previously waiting processes
prop_demonitors_previously_waiting_processes() ->
    ?FORALL(Size, range(5,100),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			
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
			fun() ->
				MonitorCount = length(get_monitors(Pool)),
				MonitorCount = 2 
			end(),
			poolboy:checkin(Pool, Worker),
			timer:sleep(500),
			fun() ->
				MonitorCount = length(get_monitors(Pool)),
				MonitorCount = 0 
			end(),
			Pid ! ok,
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_demonitors_when_checkout_cancelled
%% @doc Pool demonitors when a checkout is cancelled
prop_demonitors_when_checkout_cancelled() ->
    ?FORALL(Size, range(5,100),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			
			Self = self(),
			Pid = spawn(fun() ->
				poolboy:checkout(Pool),
				_ = (catch poolboy:checkout(Pool, true, 1000)),
				Self ! ok,
				receive ok -> ok end
			end),
			
			timer:sleep(500),

			fun() ->
				MonitorCount = length(get_monitors(Pool)),
				MonitorCount = 2 
			end(),		

			receive ok -> ok end,
			fun() ->
				MonitorCount = length(get_monitors(Pool)),
				MonitorCount = 2 
			end(),
			Pid ! ok,

			fun() ->
				MonitorCount = length(get_monitors(Pool)),
				%io:format("MonitorCount=~p, Size=~p~n",[MonitorCount,Size])
				MonitorCount = 0 
			end(),
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_default_strategy_lifo
%% @doc Check that LIFO is the default strategy
prop_default_strategy_lifo() ->
    ?FORALL(Size, range(5,100),
        begin
			%% Default strategy is LIFO
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			Worker = poolboy:checkout(Pool),
			ok = poolboy:checkin(Pool, Worker),
			Worker = poolboy:checkout(Pool),
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_lifo_strategy
%% @doc Check LIFO strategy
prop_lifo_strategy() ->
    ?FORALL(Size, range(5,100),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow, lifo),
			Worker = poolboy:checkout(Pool),
			ok = poolboy:checkin(Pool, Worker),
			Worker = poolboy:checkout(Pool),
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_fifo_strategy
%% @doc Check FIFO strategy
prop_fifo_strategy() ->
    ?FORALL(Size, range(2,100),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow, fifo),
			Worker1 = poolboy:checkout(Pool),
			ok = poolboy:checkin(Pool, Worker1),
			Worker2 = poolboy:checkout(Pool),
			true = Worker1 =/= Worker2,
			
			Workers = [poolboy:checkout(Pool) || _ <- lists:seq(0, Size-2)],
			Worker1 = hd(lists:reverse(Workers)),
			
			ok == poolboy:stop(Pool)
        end
	).
 
%% shell command for a test: rebar3 proper -n 1 -p prop_reuses_waiting_monitor_on_worker_exit
%% @doc Pool reuses waiting monitor when a worker exits
prop_reuses_waiting_monitor_on_worker_exit() ->
    ?FORALL(Size, range(1,100),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
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
			MonitorCount = length(get_monitors(Pool)),
			%io:format("MonitorCount=~p, Size=~p~n",[MonitorCount,Size])
			MonitorCount = 1,
			Pid ! ok,		
			
			ok == poolboy:stop(Pool)
        end
	).

%% shell command for a test: rebar3 proper -n 1 -p prop_transaction_timeout_without_exit
%% @doc Recover from timeout without exit handling
prop_transaction_timeout_without_exit() ->
    ?FORALL(Size, range(1,100),
	%?FORALL(_Bool, boolean(),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			fun() ->
				StateName = ready, 
				QueueLength = Size, 
				Overflow = 0, 
				CurrentSize = 0,
				{StateName, QueueLength, Overflow, CurrentSize} = pool_call(Pool, status)
			end(),
			WorkerList = pool_call(Pool, get_all_workers),
			true = is_list(WorkerList),
			true = 0 < length(WorkerList),
			
			%io:format("WorkerList=~p~n",[WorkerList]),
			spawn(poolboy, transaction, [Pool,
				fun(Worker) ->
					ok = pool_call(Worker, work)
				end,
				0]),
			timer:sleep(100),
			WorkerList = pool_call(Pool, get_all_workers),
			fun() ->
				StateName = ready, 
				QueueLength = Size, 
				Overflow = 0, 
				CurrentSize = 0,
				{StateName, QueueLength, Overflow, CurrentSize} = pool_call(Pool, status)
			end(),
			
			ok == poolboy:stop(Pool)
        end
	).	

%% shell command for a test: rebar3 proper -n 1 -p prop_transaction_timeout
%% @doc Recover from transaction timeout
prop_transaction_timeout() ->
    ?FORALL(Size, range(1,100),
	%?FORALL(_Bool, boolean(),
        begin
			MaxOverflow = 0,
			{ok, Pool} = new_pool(Size, MaxOverflow),
			fun() ->
				StateName = ready, 
				QueueLength = Size, 
				Overflow = 0, 
				CurrentSize = 0,
				{StateName, QueueLength, Overflow, CurrentSize} = pool_call(Pool, status)
			end(),
			WorkerList = pool_call(Pool, get_all_workers),
			true = is_list(WorkerList),
			true = 0 < length(WorkerList),
			
			ok = try
				poolboy:transaction(Pool,
					fun(Worker1) ->
						ok = pool_call(Worker1, work)
					end,
					0)
			catch exit:{timeout,_} -> 
				ok
			end,
			
			
			WorkerList = pool_call(Pool, get_all_workers),
			fun() ->
				StateName = ready, 
				QueueLength = Size, 
				Overflow = 0, 
				CurrentSize = 0,
				{StateName, QueueLength, Overflow, CurrentSize} = pool_call(Pool, status)
			end(),
			
			ok == poolboy:stop(Pool)
        end
	).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%
new_pool(Size, MaxOverflow) ->
    poolboy:start_link([{name, {local, poolboy_test}},
                        {worker_module, poolboy_test_worker},
                        {size, Size}, {max_overflow, MaxOverflow}]).




new_pool(Size, MaxOverflow, Strategy) ->
    poolboy:start_link([{name, {local, poolboy_test}},
                        {worker_module, poolboy_test_worker},
                        {size, Size}, {max_overflow, MaxOverflow},
                        {strategy, Strategy}]).

pool_call(ServerRef, Request) ->
    gen_server:call(ServerRef, Request).

checkin_worker(Pool, Worker) ->
    %% There's no easy way to wait for a checkin to complete, because it's
    %% async and the supervisor may kill the process if it was an overflow
    %% worker. The only solution seems to be a nasty hardcoded sleep.
    poolboy:checkin(Pool, Worker),
    timer:sleep(500).

%% Tell a worker to exit and await its impending doom.
kill_worker(Pid) ->
    erlang:monitor(process, Pid),
    pool_call(Pid, die),
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    end.

get_monitors(Pid) ->
    %% Synchronise with the Pid to ensure it has handled all expected work.
    _ = sys:get_status(Pid),
    [{monitors, Monitors}] = erlang:process_info(Pid, [monitors]),
    Monitors.

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%

