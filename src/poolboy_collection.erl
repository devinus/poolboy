-module(poolboy_collection).

-export([from/2, out/1, len/1, nth/2, prep/2, app/2, filter/2, replace/4, to/1]).

-ifdef(pre17).
-type pid_queue() :: queue().
-type pid_queue(A) :: queue(A).
-else.
-type pid_queue() :: queue:queue().
-type pid_queue(A) :: queue:queue(A).
-endif.
-export_type([pid_queue/0, pid_queue/1]).

-type coll_data() :: list()|{}|array:array()|pid_queue().
-type coll_data(A) :: list(A)|{A}|array:array(A)|pid_queue(A).

-record(typed_data, {
          type :: 'list' |'array' |'queue' |'tuple',
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
    #typed_data{type = T, data = list_to_tuple(List)}.


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
    end.

out(TD, {V, D}) -> {V, data(TD, D)}.


len(#typed_data{data = Data, type = list}) -> length(Data);
len(#typed_data{data = Data, type = queue}) -> queue:len(Data);
len(#typed_data{data = Data, type = array}) -> array:sparse_size(Data);
len(#typed_data{data = Data, type = tuple}) -> tuple_size(Data).


nth(Index, #typed_data{data = Data, type = list}) -> lists:nth(Index, Data);
nth(Index, #typed_data{data = Data, type = queue}) -> queue_nth(Index, Data);
nth(Index, #typed_data{data = Data, type = array}) -> array:get(Index-1, Data);
nth(Index, #typed_data{data = Data, type = tuple}) -> element(Index, Data).


prep(In, TD = #typed_data{data = Data, type = list}) ->
    data(TD, [In | Data]);
prep(In, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue:cons(In, Data));
prep(In, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array_prep(In, Data));
prep(In, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, erlang:insert_element(1, Data, In)).


app(In, TD = #typed_data{data = Data, type = list}) ->
    data(TD, Data ++ [In]);
app(In, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue:in(In, Data));
app(In, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array:set(array:size(Data), In, Data));
app(In, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, erlang:append_element(Data, In)).


filter(Fun, TD = #typed_data{data = Data, type = list}) ->
    data(TD, list_filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue:filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array_filter(Fun, Data));
filter(Fun, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, tuple_filter(Fun, Data)).


replace(Out, Index, In, TD = #typed_data{data = Data, type = list}) ->
    data(TD, list_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = queue}) ->
    data(TD, queue_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = array}) ->
    data(TD, array_replace(Out, Index, In, Data));
replace(Out, Index, In, TD = #typed_data{data = Data, type = tuple}) ->
    data(TD, tuple_replace(Out, Index, In, Data)).


to(#typed_data{data = Data, type = list}) -> Data;
to(#typed_data{data = Data, type = queue}) -> queue:to_list(Data);
to(#typed_data{data = Data, type = array}) -> array:to_list(Data);
to(#typed_data{data = Data, type = tuple}) -> tuple_to_list(Data).


data(TD, Data) -> TD#typed_data{data = Data}.



list_filter(Fun, L) ->
    list_filter(Fun, L, []).

list_filter(_Fun, [], Acc) -> lists:reverse(Acc);
list_filter(Fun, [H | T], Acc) ->
    case Fun(H) of
        true -> list_filter(Fun, T, [H | Acc]);
        false -> list_filter(Fun, T, Acc);
        H -> list_filter(Fun, T, [H | Acc]);
        Else -> list_filter(Fun, T, [Else | Acc])
    end.

list_replace(O, X, I, L) ->
    {L1, [O | Tl]} = lists:split(X-1, L),
    L1 ++ [I| Tl].



array_prep(I, A) ->
    array:foldl(
      fun(Idx, Val, Arr) ->
              array:set(Idx+1, Val, Arr)
      end,
      array:set(0, I, array:new()),
      A).


array_filter(F, A) ->
    array:sparse_map(
      fun(_, V) ->
              case F(V) of
                  true -> V;
                  false -> array:default(A);
                  Else -> Else
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
        Else when Else == Element -> Tuple;
        false -> erlang:delete_element(Index, Tuple);
        Else -> setelement(Index, Tuple, Else)
    end,
    tuple_filter(Fun, NewTuple, Index-1).


tuple_replace(O, X, I, Tu) ->
    O = element(X, Tu),
    setelement(X, Tu, I).
