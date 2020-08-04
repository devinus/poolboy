-module(poolboy_worker_collection).

-export([new/4,
         length/2,
         hide_head/1,
         replace/2, replace/3,
         lifo/2, prepend/2,
         fifo/2, append/2,
         filter/2,
         all/2,
         rand/2
        ]).


-record(coll, {
          item_generator :: fun((non_neg_integer()) -> any()),
          data :: poolboy_collection:typed_data() | poolboy_collection:typed_data(any()),
          indexes :: poolboy_collection:typed_data() | poolboy_collection:typed_data(any()),
          rev_indexes :: #{any()=>non_neg_integer()}
          }).

-type coll() :: #coll{
                   data :: poolboy_collection:typed_data(),
                   rev_indexes :: #{}
                  }.
-type coll(A) :: #coll{
                    item_generator :: fun((non_neg_integer()) -> A),
                    data :: poolboy_collection:typed_data(A),
                    rev_indexes :: #{A=>non_neg_integer()}
                    }.

-export_type([coll/0, coll/1]).


new(Type, Size, lifo, Fun) -> new(Type, Size, list, Fun);
new(Type, Size, fifo, Fun) -> new(Type, Size, queue, Fun);
new(Type, Size, IndexesType, Fun) when is_function(Fun, 1) ->
    Indexes = lists:seq(1, Size),
    Items = [Fun(I) || I <- Indexes],
    RevIndexes = maps:from_list(lists:zip(Items, Indexes)),
    #coll{
       item_generator = Fun,
       data = poolboy_collection:from(Items, Type),
       indexes = poolboy_collection:from(Indexes, IndexesType),
       rev_indexes = RevIndexes
      }.


length(known, #coll{data=Data}) -> poolboy_collection:len(Data);
length(visible, #coll{indexes=Indexes}) -> poolboy_collection:len(Indexes).


hide_head(Coll = #coll{indexes = Indexes, data=Data}) ->
    case poolboy_collection:out(Indexes) of
        {empty, _} -> empty;
        {{value, Hd}, Tl} ->
            {poolboy_collection:nth(Hd, Data), Coll#coll{indexes = Tl}}
    end.



replace(Out, Coll = #coll{item_generator = In}) ->
    replace(Out, In, Coll).

replace(Out, In, Coll) when not is_function(In, 1) ->
    replace(Out, fun(_) -> In end, Coll);
replace(Out, In, Coll = #coll{data = Data}) ->
    case maps:take(Out, Coll#coll.rev_indexes) of
        error -> error(enoent);
        {OutIndex, RevIndexes} ->
            NewItem = In(OutIndex),
            NewData = poolboy_collection:replace(Out, OutIndex, NewItem, Data),
            NewRevIndexes = maps:put(NewItem, OutIndex, RevIndexes),
            {NewItem, Coll#coll{rev_indexes = NewRevIndexes, data = NewData}}
    end.

lifo(In, Coll) -> prepend(In, Coll).

prepend(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = _Data}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes = poolboy_collection:prep(InIndex, Indexes)}
    end.


fifo(In, Coll) -> append(In, Coll).

append(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = _Data}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes = poolboy_collection:app(InIndex, Indexes)}
    end.

filter(Fun, #coll{data = Data}) ->
    poolboy_collection:filter(Fun, Data).

all(known, #coll{rev_indexes = RevIndexes}) ->
    maps:keys(RevIndexes);
all(visible, #coll{indexes = Indexes, data = Data}) ->
    poolboy_collection:to(poolboy_collection:filter(fun(I) -> [poolboy_collection:nth(I, Data)] end, Indexes)).


rand(known, #coll{data = Data}) ->
    case poolboy_collection:len(Data) of
        0 -> empty;
        L -> poolboy_collection:nth(rand:uniform(L), Data)
    end;
rand(visible, #coll{indexes = Indexes, data = Data}) ->
    case poolboy_collection:len(Indexes) of
        0 -> empty;
        L -> poolboy_collection:nth(poolboy_collection:nth(rand:uniform(L), Indexes), Data)
    end.
