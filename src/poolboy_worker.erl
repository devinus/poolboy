-module (poolboy_worker).
-author ('Dietrich Featherston <d@d2fn.com>').
-export ([call/2,call/3]).
-export ([cast/2]).
-define (TIMEOUT,5000).

%%------------------------------------------------------------
%% worker pooling
%%------------------------------------------------------------

cast(Pool,Msg) ->
  worker_send(Pool,Msg,cast).

call(Pool,Msg) ->
  call(Pool,Msg,?TIMEOUT).

call(Pool,Msg,Timeout) ->
  worker_send(Pool,Msg,call,Timeout).

worker_send(Pool,Msg,SendFun) ->
  worker_send(Pool,Msg,SendFun,?TIMEOUT).

worker_send(Pool,Msg,SendFun,Timeout) ->
  Worker = poolboy:checkout(Pool),
  try
    case SendFun of
      call  ->
        gen_server:SendFun(Worker,Msg,Timeout);
      _     ->
        gen_server:SendFun(Worker,Msg)
    end
  catch
    E ->
      error_logger:error_msg("error during gen_server:~p : ~p~n",[SendFun,E])
  after
    case is_process_alive(Worker) of
      true ->
        poolboy:checkin(Pool,Worker);
      _ ->
        error_logger:error_msg("worker died during gen_server:~p, evicting from pool~n",[SendFun])
    end
  end.
