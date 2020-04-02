-module(poolboy_collection).

-export([new/3,
         length/2,
         hide_head/1,
         replace/2, replace/3,
         prepend/2,
         append/2,
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

-record(type, {
          from :: fun((list(A)) -> coll_data(A)),
          is :: fun((coll_data() | coll_data(any())) -> boolean()),
          len :: fun((coll_data() | coll_data(any())) -> non_neg_integer()),
          nth :: fun((non_neg_integer(), coll_data(A)) -> A),
          prep :: fun((A, coll_data(A)) -> coll_data(A)),
          app :: fun((A, coll_data(A)) -> coll_data(A)),
          filter :: fun((fun((A) -> boolean()), coll_data(A)) -> coll_data(A)),
          replace :: fun((A, non_neg_integer(), A, coll_data(A)) -> coll_data(A))
         }).
-type type() :: #type{}.

-record(coll, {
          item_generator :: fun((non_neg_integer()) -> any()),
          data :: coll_data() | coll_data(any()),
          type :: type(),
          indexes :: [non_neg_integer()],
          rev_indexes :: #{any()=>non_neg_integer()}
          }).

-type coll() :: #coll{
                   data :: coll_data(),
                   rev_indexes :: #{}
                  }.
-type coll(A) :: #coll{
                    item_generator :: fun((non_neg_integer()) -> A),
                    data :: coll_data(A),
                    rev_indexes :: #{A=>non_neg_integer()}
                    }.

-export_type([coll/0, coll/1]).


-define(TYPES, #{
          list => #type{
                     from = fun(L) -> L end,
                     is = fun is_list/1,
                     len = fun length/1,
                     nth = fun lists:nth/2,
                     prep = fun(I, L) -> [I|L] end,
                     app = fun(I, L) -> L ++ [I] end,
                     filter = fun lists:filter/2,
                     replace = fun list_replace/4
                    },
          array => #type{
                      from = fun array:from_list/1,
                      is = fun array:is_array/1,
                      len = fun array:size/1,
                      nth = fun(I, A) -> array:get(I-1, A) end,
                      prep = fun array_prep/2,
                      app = fun(I, A) -> array:set(array:size(A), I, A) end,
                      filter = fun array_filter/2,
                      replace = fun array_replace/4
                     },
          queue => #type{
                      from = fun queue:from_list/1,
                      is = fun queue:is_queue/1,
                      len = fun queue:len/1,
                      nth = fun queue_nth/2,
                      prep = fun queue:in_r/2,
                      app = fun queue:in/2,
                      filter = fun queue:filter/2,
                      replace = fun queue_replace/4
                     },
          tuple => #type{
                      from = fun list_to_tuple/1,
                      is = fun is_tuple/1,
                      len = fun tuple_size/1,
                      nth = fun element/2,
                      prep = fun(I, Tu) ->  erlang:insert_element(1, Tu, I) end,
                      app = fun(I, Tu) -> erlang:append_element(Tu, I) end,
                      filter = fun tuple_filter/2,
                      replace = fun tuple_replace/4
                     }
         }).

from(Data, T) -> (T#type.from)(Data).
is(Data, T) -> (T#type.is)(Data).
len(Data, T) -> (T#type.len)(Data).
nth(Index, Data, T) -> (T#type.nth)(Index, Data).
prep(In, Data, T) -> (T#type.prep)(In, Data).
app(In, Data, T) -> (T#type.app)(In, Data).
filter(Fun, Data, T) -> (T#type.filter)(Fun, Data).
replace(In, Index, Out, Data, T) -> (T#type.replace)(In, Index, Out, Data).


new(Type, Size, Fun) when is_function(Fun, 1) ->
    CollType = maps:get(Type, ?TYPES),
    Indexes = lists:seq(1, Size),
    Items = [Fun(I) || I <- Indexes],
    RevIndexes = maps:from_list(lists:zip(Items, Indexes)),
    #coll{
       item_generator = Fun,
       data = from(Items, CollType),
       type = CollType,
       indexes = Indexes,
       rev_indexes = RevIndexes
      }.


length(known, #coll{data=Data, type=T}) -> len(Data, T);
length(visible, #coll{indexes=Indexes}) -> length(Indexes).


hide_head(#coll{indexes = []}) -> empty;
hide_head(Coll = #coll{indexes = [Hd|Tl], data=Data, type=T}) ->
    {nth(Hd, Data, T), Coll#coll{indexes = Tl}}.


replace(Out, Coll = #coll{item_generator = In}) ->
    replace(Out, In, Coll).

replace(Out, In, Coll) when not is_function(In, 1) ->
    replace(Out, fun(_) -> In end, Coll);
replace(Out, In, Coll = #coll{data = Data, type = T}) ->
    case maps:take(Out, Coll#coll.rev_indexes) of
        error -> error(enoent);
        {OutIndex, RevIndexes} ->
            NewData = replace(Out, OutIndex, In(OutIndex), Data, T),
            NewItem = nth(OutIndex, NewData, T),
            NewRevIndexes = maps:put(NewItem, OutIndex, RevIndexes),
            {NewItem, Coll#coll{rev_indexes = NewRevIndexes, data = NewData}}
    end.


prepend(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data, type = T}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes=[InIndex|Indexes]};
        undefined ->
            NewData = prep(In, Data, T),
            NewRevIndexes = maps:put(In, 1, maps:map(fun(_, V) -> V + 1 end, RevIndexes)),
            NewIndexes = [1 | [I+1 || I <- Indexes]],
            Coll#coll{indexes = NewIndexes, rev_indexes = NewRevIndexes, data = NewData}
    end.



append(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data, type = T}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes=Indexes++[InIndex]};
        undefined ->
            NewData = app(In, Data, T),
            ArrayT = maps:get(array, ?TYPES),
            NewIndex =
            case {is(Data, ArrayT), len(Data, T)} of
                {true, Len} -> Len - 1;
                {_, Len} -> Len
            end,
            NewRevIndexes = maps:put(In, NewIndex, RevIndexes),
            NewIndexes = Indexes++[NewIndex],
            Coll#coll{indexes = NewIndexes, rev_indexes = NewRevIndexes, data = NewData}
    end.


filter(Fun, #coll{data = Data, type = T}) ->
    filter(Fun, Data, T).


all(known, #coll{rev_indexes = RevIndexes}) ->
    maps:keys(RevIndexes);
all(visible, #coll{indexes = Indexes, data = Data, type = T}) ->
    [nth(I, Data, T) || I <- Indexes].


rand(known, #coll{data = Data, type = T}) ->
    case len(Data, T) of
        0 -> empty;
        L -> nth(rand:uniform(L), Data, T)
    end;
rand(visible, #coll{indexes = []}) -> empty;
rand(visible, #coll{indexes = Indexes, data = Data, type = T}) ->
    nth(lists:nth(rand:uniform(length(Indexes)), Indexes), Data, T).





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



queue_nth(I, {_RL, FL}) when I =< length(FL) ->
    lists:nth(I, FL);
queue_nth(I, {RL, FL}) ->
    J = length(RL)-(I-length(FL)-1),
    lists:nth(J, RL).


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
