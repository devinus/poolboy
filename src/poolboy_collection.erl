-module(poolboy_collection).

-export([new/3,
         length/2,
         hide_head/1,
         replace/2, replace/3,
         lifo/2, prepend/2,
         fifo/2, append/2,
         filter/2,
         all/2,
         rand/2
        ]).

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

-record(type, {
          len :: fun((coll_data() | coll_data(any())) -> non_neg_integer()),
          nth :: fun((non_neg_integer(), coll_data(A)) -> A),
          prep :: fun((A, coll_data(A)) -> coll_data(A)),
          app :: fun((A, coll_data(A)) -> coll_data(A)),
          filter :: fun((fun((A) -> boolean()), coll_data(A)) -> coll_data(A)),
          replace :: fun((A, non_neg_integer(), A, coll_data(A)) -> coll_data(A))
         }).

-record(coll, {
          item_generator :: fun((non_neg_integer()) -> any()),
          data :: typed_data() | typed_data(any()),
          indexes :: [non_neg_integer()],
          rev_indexes :: #{any()=>non_neg_integer()}
          }).

-type coll() :: #coll{
                   data :: typed_data(),
                   rev_indexes :: #{}
                  }.
-type coll(A) :: #coll{
                    item_generator :: fun((non_neg_integer()) -> A),
                    data :: typed_data(A),
                    rev_indexes :: #{A=>non_neg_integer()}
                    }.

-export_type([coll/0, coll/1]).


-define(TYPES(T),
        case T of
          list -> #type{
                     len = fun length/1,
                     nth = fun lists:nth/2,
                     prep = fun(I, L) -> [I|L] end,
                     app = fun(I, L) -> L ++ [I] end,
                     filter = fun lists:filter/2,
                     replace = fun list_replace/4
                    };
          array -> #type{
                      len = fun array:size/1,
                      nth = fun(I, A) -> array:get(I-1, A) end,
                      prep = fun array_prep/2,
                      app = fun(I, A) -> array:set(array:size(A), I, A) end,
                      filter = fun array_filter/2,
                      replace = fun array_replace/4
                     };
          queue -> #type{
                      len = fun queue:len/1,
                      nth = fun queue_nth/2,
                      prep = fun queue:in_r/2,
                      app = fun queue:in/2,
                      filter = fun queue:filter/2,
                      replace = fun queue_replace/4
                     };
          tuple -> #type{
                      len = fun tuple_size/1,
                      nth = fun element/2,
                      prep = fun(I, Tu) ->  erlang:insert_element(1, Tu, I) end,
                      app = fun(I, Tu) -> erlang:append_element(Tu, I) end,
                      filter = fun tuple_filter/2,
                      replace = fun tuple_replace/4
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


len(#typed_data{type = T, data = Data}) ->
    (?TYPES(T)#type.len)(Data).
nth(Index, #typed_data{type = T, data = Data}) ->
    (?TYPES(T)#type.nth)(Index, Data).
prep(In, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.prep)(In, Data)}.
app(In, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.app)(In, Data)}.
filter(Fun, #coll{data = Data}) -> filter(Fun, Data);
filter(Fun, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.filter)(Fun, Data)}.
replace(Out, Index, In, TD = #typed_data{type = T, data = Data}) ->
    TD#typed_data{data = (?TYPES(T)#type.replace)(Out, Index, In, Data)}.


new(Type, Size, Fun) when is_function(Fun, 1) ->
    Indexes = lists:seq(1, Size),
    Items = [Fun(I) || I <- Indexes],
    RevIndexes = maps:from_list(lists:zip(Items, Indexes)),
    #coll{
       item_generator = Fun,
       data = from(Items, Type),
       indexes = Indexes,
       rev_indexes = RevIndexes
      }.


length(known, #coll{data=Data}) -> len(Data);
length(visible, #coll{indexes=Indexes}) -> length(Indexes).


hide_head(#coll{indexes = []}) -> empty;
hide_head(Coll = #coll{indexes = [Hd|Tl], data=Data}) ->
    {nth(Hd, Data), Coll#coll{indexes = Tl}}.


replace(Out, Coll = #coll{item_generator = In}) ->
    replace(Out, In, Coll).

replace(Out, In, Coll) when not is_function(In, 1) ->
    replace(Out, fun(_) -> In end, Coll);
replace(Out, In, Coll = #coll{data = Data}) ->
    case maps:take(Out, Coll#coll.rev_indexes) of
        error -> error(enoent);
        {OutIndex, RevIndexes} ->
            NewItem = In(OutIndex),
            NewData = replace(Out, OutIndex, NewItem, Data),
            NewRevIndexes = maps:put(NewItem, OutIndex, RevIndexes),
            {NewItem, Coll#coll{rev_indexes = NewRevIndexes, data = NewData}}
    end.

lifo(In, Coll) -> prepend(In, Coll).

prepend(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes=[InIndex|Indexes]};
        undefined ->
            NewData = prep(In, Data),
            NewRevIndexes = maps:put(In, 1, maps:map(fun(_, V) -> V + 1 end, RevIndexes)),
            NewIndexes = [1 | [I+1 || I <- Indexes]],
            Coll#coll{indexes = NewIndexes, rev_indexes = NewRevIndexes, data = NewData}
    end.


fifo(In, Coll) -> append(In, Coll).

append(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes=Indexes++[InIndex]};
        undefined ->
            NewData = app(In, Data),
            NewIndex =
            case {Data#typed_data.type, len(Data)} of
                {array, Len} -> Len - 1;
                {_, Len} -> Len
            end,
            NewRevIndexes = maps:put(In, NewIndex, RevIndexes),
            NewIndexes = Indexes++[NewIndex],
            Coll#coll{indexes = NewIndexes, rev_indexes = NewRevIndexes, data = NewData}
    end.


all(known, #coll{rev_indexes = RevIndexes}) ->
    maps:keys(RevIndexes);
all(visible, #coll{indexes = Indexes, data = Data}) ->
    [nth(I, Data) || I <- Indexes].


rand(known, #coll{data = Data}) ->
    case len(Data) of
        0 -> empty;
        L -> nth(rand:uniform(L), Data)
    end;
rand(visible, #coll{indexes = []}) -> empty;
rand(visible, #coll{indexes = Indexes, data = Data}) ->
    nth(lists:nth(rand:uniform(length(Indexes)), Indexes), Data).





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
    tuple_filter(Fun, Tuple, erlang:tuple_size(Tuple)).

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
