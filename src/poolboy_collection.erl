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

-record(type, {
          out :: fun((coll_data() | coll_data(any())) -> {{'value', any()}, coll_data(any())} | {'empty', coll_data()}),
          len :: fun((coll_data() | coll_data(any())) -> non_neg_integer()),
          nth :: fun((non_neg_integer(), coll_data(A)) -> A),
          prep :: fun((A, coll_data(A)) -> coll_data(A)),
          app :: fun((A, coll_data(A)) -> coll_data(A)),
          filter :: fun((fun((A) -> boolean()), coll_data(A)) -> coll_data(A)),
          replace :: fun((A, non_neg_integer(), A, coll_data(A)) -> coll_data(A)),
          to :: fun((coll_data() | coll_data(any())) -> [any()])
         }).

-define(TYPES(T),
        case T of
          list -> #type{
                      out = fun([] = L) -> {empty, L};
                               ([Hd|Tl]) -> {{value, Hd}, Tl} end,
                      len = fun length/1,
                      nth = fun lists:nth/2,
                      prep = fun(I, L) -> [I|L] end,
                      app = fun(I, L) -> L ++ [I] end,
                      filter = fun lists:filter/2,
                      replace = fun list_replace/4,
                      to = fun(L) -> L end
                     };
          array -> #type{
                      out = fun(A) ->
                                case array:size(A) of
                                    0 -> {empty, A};
                                    _ -> {{value, array:get(0, A)},
                                          array:reset(0, A)}
                                end
                            end,
                      len = fun array:size/1,
                      nth = fun(I, A) -> array:get(I-1, A) end,
                      prep = fun array_prep/2,
                      app = fun(I, A) -> array:set(array:size(A), I, A) end,
                      filter = fun array_filter/2,
                      replace = fun array_replace/4,
                      to = fun array:to_list/1
                     };
          queue -> #type{
                      out = fun queue:out/1,
                      len = fun queue:len/1,
                      nth = fun queue_nth/2,
                      prep = fun queue:in_r/2,
                      app = fun queue:in/2,
                      filter = fun queue:filter/2,
                      replace = fun queue_replace/4,
                      to = fun queue:to_list/1
                     };
          tuple -> #type{
                      out = fun(Tu) ->
                                case erlang:tuple_size(Tu) of
                                    0 -> {empty, Tu};
                                    _ -> {{value, element(1, Tu)},
                                            erlang:delete_element(1, Tu)}
                                end
                            end,
                      len = fun tuple_size/1,
                      nth = fun element/2,
                      prep = fun(I, Tu) ->  erlang:insert_element(1, Tu, I) end,
                      app = fun(I, Tu) -> erlang:append_element(Tu, I) end,
                      filter = fun tuple_filter/2,
                      replace = fun tuple_replace/4,
                      to = fun erlang:tuple_to_list/1
                     }
        end).

from(List, T) when T == list ->
    #typed_data{type = T, data = List};
from(List, T) when T == queue ->
    #typed_data{type = T, data = queue:from_list(List)};
from(List, T) when T == array ->
    #typed_data{type = T, data = array:from_list(List)};
from(List, T) when T == tuple ->
    #typed_data{type = T, data = list_to_tuple(List)}.


out(TD = #typed_data{type = T, data = Data}) ->
    {V, D} = (?TYPES(T)#type.out)(Data),
    {V, TD#typed_data{data = D}}.
len(#typed_data{type = T, data = Data}) ->
    (?TYPES(T)#type.len)(Data).
nth(Index, #typed_data{type = T, data = Data}) ->
    (?TYPES(T)#type.nth)(Index, Data).
prep(In, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.prep)(In, Data)}.
app(In, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.app)(In, Data)}.
filter(Fun, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.filter)(Fun, Data)}.
replace(Out, Index, In, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.replace)(Out, Index, In, Data)}.
to(#typed_data{type = T, data = Data}) ->
    (?TYPES(T)#type.to)(Data).




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
        false -> setelement(Index, Tuple, undefined);
        Else -> setelement(Index, Tuple, Else)
    end,
    tuple_filter(Fun, NewTuple, Index-1).


tuple_replace(O, X, I, Tu) ->
    O = element(X, Tu),
    setelement(X, Tu, I).
