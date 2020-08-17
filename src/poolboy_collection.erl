-module(poolboy_collection).

-include_lib("stdlib/include/ms_transform.hrl").

-export([from/2, out/1, len/1, nth/2, add/2, filter/2, replace/4, to/1]).

-ifdef(pre17).
-type pid_queue() :: queue().
-type pid_queue(A) :: queue(A).
-else.
-type pid_queue() :: queue:queue().
-type pid_queue(A) :: queue:queue(A).
-endif.
-export_type([pid_queue/0, pid_queue/1]).

-type coll_data() :: list()|{}|array:array()|pid_queue()|ets:tid().
-type coll_data(A) :: list(A)|{A}|array:array(A)|pid_queue(A)|ets:tid().

-record(typed_data, {
          type :: 'list' |'array' |'queue' |'tuple' |'ets',
          data :: coll_data() | coll_data(any())
         }).
-type typed_data() :: #typed_data{data :: coll_data()}.
-type typed_data(A) :: #typed_data{data :: coll_data(A)}.
-export_type([typed_data/0, typed_data/1]).


from(List, T = list) ->
    #typed_data{type = T, data = List};
from(List, T = queue) ->
    #typed_data{type = T, data = queue:from_list(List)};
from(List, T = array) ->
    #typed_data{type = T, data = array:from_list(List)};
from(List, T = tuple) ->
    #typed_data{type = T, data = list_to_tuple(List)};
from(List, T = ets) ->
    Tab = ets:new(table, [ordered_set]),
    true = ets:insert_new(Tab, [{I} || I <- List]),
    #typed_data{type = T, data = Tab}.


out(TD = #typed_data{data = Data, type = list}) ->
    case Data of
        [] = L -> out(TD, {empty, L});
        [H|T] -> out(TD, {{value, H}, T})
    end;
out(TD = #typed_data{data = Data, type = queue}) ->
    out(TD, queue:out(Data));
out(TD = #typed_data{data = Data, type = array}) ->
    case array:sparse_size(Data) of
        0 -> out(TD, {empty, Data});
        _ -> out(TD, {{value, array:get(0, Data)}, array:reset(0, Data)})
    end;
out(TD = #typed_data{data = Data, type = tuple}) ->
    case erlang:tuple_size(Data) of
        0 -> out(TD, {empty, Data});
        _ -> out(TD, {{value, element(1, Data)}, erlang:delete_element(1, Data)})
    end;
out(TD = #typed_data{data = Data, type = ets}) ->
    case ets:info(Data, size) of
        0 -> {empty, TD};
        Size ->
            Index = rand:uniform(Size),
            [{Value}] = ets:slot(Data, Index-1),
            true = ets:delete(Data, Value),
            {{value, Value}, TD}
    end.

out(TD, {V, D}) -> {V, data(TD, D)}.


len(#typed_data{data = Data, type = list}) -> length(Data);
len(#typed_data{data = Data, type = queue}) -> queue:len(Data);
len(#typed_data{data = Data, type = array}) -> array:sparse_size(Data);
len(#typed_data{data = Data, type = tuple}) -> tuple_size(Data);
len(#typed_data{data = Data, type = ets}) -> ets:info(Data, size).


nth(Index, #typed_data{data = Data, type = list}) -> lists:nth(Index, Data);
nth(Index, #typed_data{data = Data, type = queue}) -> queue_nth(Index, Data);
nth(Index, #typed_data{data = Data, type = array}) -> array:get(Index-1, Data);
nth(Index, #typed_data{data = Data, type = tuple}) -> element(Index, Data);
nth(Index, #typed_data{data = Data, type = ets}) ->
    [{Value}] = ets:slot(Data, Index-1),
    Value.


add(In, TD = #typed_data{data = Data, type = list}) ->
    data(TD, [In | Data]);
add(In, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue:in(In, Data));
add(In, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array:set(array:size(Data), In, Data));
add(In, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, erlang:append_element(Data, In));
add(In, TD = #typed_data{data = Data, type = ets}) ->
    true = ets:insert_new(Data, {In}),
    TD.


filter(Fun, TD = #typed_data{data = Data, type = list}) ->
    data(TD, list_filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue:filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array_filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, tuple_filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = ets}) ->
    data(TD, ets_filter(Fun, Data)).


replace(Out, Index, In, TD = #typed_data{data = Data, type = list}) ->
    data(TD, list_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, tuple_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = ets}) ->
    [{Out}] = ets:slot(Data, Index),
    [{Out}] = ets:take(Data, Out),
    true = ets:insert_new(Data, {In}),
    TD.


to(#typed_data{data = Data, type = list}) -> Data;
to(#typed_data{data = Data, type = queue}) -> queue:to_list(Data);
to(#typed_data{data = Data, type = array}) -> array:to_list(Data);
to(#typed_data{data = Data, type = tuple}) -> tuple_to_list(Data);
to(#typed_data{data = Data, type = ets}) ->
    ets:select(Data, ets:fun2ms(fun({I}) -> I end)).


data(TD, Data) -> TD#typed_data{data = Data}.



list_filter(Fun, L) ->
    list_filter(Fun, L, []).

list_filter(_Fun, [], Acc) -> lists:reverse(Acc);
list_filter(Fun, [H | T], Acc) ->
    case Fun(H) of
        true -> list_filter(Fun, T, [H | Acc]);
        [Else] when Else == H -> list_filter(Fun, T, [H | Acc]);
        false -> list_filter(Fun, T, Acc);
        [] -> list_filter(Fun, T, Acc);
        [Else] -> list_filter(Fun, T, [Else | Acc])
    end.

list_replace(O, X, I, L) ->
    {L1, [O | Tl]} = lists:split(X-1, L),
    L1 ++ [I| Tl].



array_filter(F, A) ->
    array:sparse_map(
      fun(_, V) ->
              case F(V) of
                  true -> V;
                  [Else] when Else == V -> V;
                  false -> array:default(A);
                  [] -> array:default(A);
                  [Else] -> Else
              end
      end,
      A).


array_replace(O, X, I, A) ->
    O = array:get(X-1, A),
    array:set(X-1, I, A).



queue_nth(I, Q) ->
    case queue:is_queue(Q) of
        true ->
            {Q1, _Q2} = queue:split(I, Q),
            queue:last(Q1);
        _ ->
            throw(badarg)
    end.

queue_replace(O, X, I, Q) ->
    {Q1, Q2} = queue:split(X-1, Q),
    O = queue:get(Q2),
    queue:join(queue:in(I, Q1), queue:drop(Q2)).



tuple_filter(Fun, Tuple) ->
    tuple_filter(Fun, Tuple, tuple_size(Tuple)).

tuple_filter(_Fun, Tuple, 0) -> Tuple;
tuple_filter(Fun, Tuple, Index) ->
    Element = element(Index, Tuple),
    NewTuple = case Fun(Element) of
        true -> Tuple;
        [Else] when Else == Element -> Tuple;
        false -> erlang:delete_element(Index, Tuple);
        [] -> erlang:delete_element(Index, Tuple);
        [Else] -> setelement(Index, Tuple, Else)
    end,
    tuple_filter(Fun, NewTuple, Index-1).


tuple_replace(O, X, I, Tu) ->
    O = element(X, Tu),
    setelement(X, Tu, I).



ets_filter(Fun, Tab) ->
    {Ins, Outs} =
    ets:foldl(
      fun({Item}, {In, Out} = Acc) ->
              case Fun(Item) of
                  true -> Acc;
                  [Else] when Else == Item -> Acc;
                  false -> {In, [Item | Out]};
                  [] -> {In, [Item | Out]};
                  [Else] -> {[Else | In], [Item | Out]}
              end
      end,
      {[], []},
      Tab),
    true = lists:min([true | [ets:delete(Tab, O) || O <- Outs]]),
    true = ets:insert_new(Tab, Ins),
    Tab.
