-module(poolboy_collection).

-export([new/3,
         len/2,
         hide_head/1,
         replace/2, replace/3,
         prepend/2,
         append/2,
         foreach/2,
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

-define(Types, 
        #{list => #{
            is => fun is_list/1,
            len => fun length/1,
            from => fun(L) -> L end,
            nth => fun lists:nth/2,
            prep => fun(I, L) -> [I|L] end,
            app => fun(I, L) -> L ++ [I] end
           },
          array => #{
            is => fun array:is_array/1,
            len => fun array:size/1,
            from => fun array:from_list/1,
            nth => fun(I, A) -> array:get(I-1, A) end,
            prep => fun(I, A) -> array:foldl(
                                   fun(Idx, Val, Arr) ->
                                           array:set(Idx+1, Val, Arr)
                                   end,
                                   array:set(0, I, array:new()),
                                   A)
                    end,
            app => fun(I, A) -> array:set(array:size(A), I, A) end
           },
          queue => #{
            is => fun queue:is_queue/1,
            len => fun queue:len/1,
            from => fun queue:from_list/1,
            nth => fun(I, {RL, FL})
                         when I =< length(FL) ->
                           lists:nth(I, FL);
                      (I, {RL, FL}) ->
                           lists:nth(
                             length(RL)-(I-length(FL)-1),
                             RL)
                   end,
            prep => fun queue:in_r/2,
            app => fun queue:in/2
           },
          tuple => #{
            is => fun is_tuple/1,
            len => fun tuple_size/1,
            from => fun list_to_tuple/1,
            nth => fun element/2,
            prep => fun(I, T) ->  erlang:insert_element(1, T, I) end,
            app => fun(I, T) -> erlang:append_element(T, I) end
           }
         }).
-define(Colls(T, F), maps:get(F, maps:get(T, ?Types))).


-record(coll, {indexes :: [non_neg_integer()],
               rev_indexes :: #{any()=>non_neg_integer()},
               data :: coll_data() | coll_data(any()),
               item_generator :: fun((non_neg_integer()) -> any()) }).

-type coll() :: #coll{rev_indexes :: #{}, data :: coll_data()}.
-type coll(A) :: #coll{rev_indexes :: #{A=>non_neg_integer()},
                       data :: coll_data(A),
                       item_generator :: fun((non_neg_integer()) -> A) }.

-export_type([coll/0, coll/1]).

new(Type, Size, Fun) when is_function(Fun, 1) ->
    Indexes = lists:seq(1, Size),
    RevIndexes = maps:from_list([{Fun(I), I} || I <- Indexes]),
    Data = (from(Type))(maps:keys(RevIndexes)),
    #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data,
          item_generator = Fun}.

from(Type) -> ?Colls(Type, ?FUNCTION_NAME).

len(known, #coll{data=Data}) -> len(Data);
len(visible, #coll{indexes=Indexes}) -> length(Indexes).

len(Data) ->
    (call(?FUNCTION_NAME, Data))(Data).

call(F, Data) when is_atom(F) ->
    call(F, Data, false, [none| maps:keys(?Types)]).

call(_, _Data, _, []) -> throw(badarg);
call(F, _Data, true, [T |_Types]) -> ?Colls(T, F);
call(F, Data, false, [_ | [T |_] = Types]) ->
    call(F, Data, ?Colls(T, is), Types);
call(F, Data, IsType, Types) when is_function(IsType, 1) ->
    call(F, Data, IsType(Data), Types).


hide_head(#coll{indexes = []}) -> empty;
hide_head(Coll = #coll{indexes = [H|T], data=Data}) ->
    {nth(H, Data), Coll#coll{indexes = T}}.

nth(Index, Data) ->
    (call(?FUNCTION_NAME, Data))(Index, Data).

replace(Out, Coll = #coll{item_generator = In}) ->
    replace(Out, In, Coll).

replace(Out, In, Coll = #coll{data = Data}) ->
    case maps:take(Out, Coll#coll.rev_indexes) of
        error -> error(enoent);
        {OutIndex, RevIndexes} ->
            NewData = data_replace(OutIndex, Out, In, Data),
            NewItem = nth(OutIndex, NewData),
            NewRevIndexes = maps:put(NewItem, OutIndex, RevIndexes),
            {NewItem, Coll#coll{rev_indexes = NewRevIndexes, data = NewData}}
    end.

data_replace(OutIndex, Out, In, Data) when not is_function(In) ->
    data_replace(OutIndex, Out, fun(_) -> In end, Data);
data_replace(OutIndex, Out, In, Data) when is_function(In, 1) andalso is_list(Data) ->
    {FirstN, [Out | Tail]} = lists:split(OutIndex-1, Data),
    FirstN ++ [In(OutIndex)| Tail];
data_replace(OutIndex, Out, In, Data) when is_function(In, 1) andalso is_tuple(Data) ->
    case array:is_array(Data) of
        true ->
            ArrIndex = OutIndex-1,
            Out = array:get(ArrIndex, Data),
            array:set(ArrIndex, In(OutIndex), Data);
        false when element(OutIndex, Data) == Out ->
            setelement(OutIndex, Data, In(OutIndex))
    end.


prepend(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes=[InIndex|Indexes]};
        undefined ->
            NewData = prep(In, Data),
            NewRevIndexes = maps:put(In, 1, maps:map(fun(_, V) -> V + 1 end, RevIndexes)),
            NewIndexes = [1 | [I+1 || I <- Indexes]],
            Coll#coll{indexes = NewIndexes, rev_indexes = NewRevIndexes, data = NewData}
    end.

prep(In, Data) ->
    (call(?FUNCTION_NAME, Data))(In, Data).

append(In, Coll = #coll{indexes = Indexes, rev_indexes = RevIndexes, data = Data}) ->
    case maps:get(In, RevIndexes, undefined) of
        InIndex when is_integer(InIndex) -> Coll#coll{indexes=Indexes++[InIndex]};
        undefined ->
            NewData = app(In, Data),
            NewIndex =
            case {array:is_array(Data), len(Data)} of
                {true, Len} -> Len - 1;
                {_, Len} -> Len
            end,
            NewRevIndexes = maps:put(In, NewIndex, RevIndexes),
            NewIndexes = Indexes++[NewIndex],
            Coll#coll{indexes = NewIndexes, rev_indexes = NewRevIndexes, data = NewData}
    end.

app(In, Data) ->
    (call(?FUNCTION_NAME, Data))(In, Data).

foreach(Fun, #coll{data = Data}) -> data_foreach(Fun, Data).

data_foreach(Fun, Data) when is_function(Fun) andalso is_list(Data) ->
    lists:foreach(Fun, Data);
data_foreach(Fun, Data) when is_function(Fun) andalso is_tuple(Data) ->
    case array:is_array(Data) of
        true -> array:sparse_map(fun(_, Value) -> Fun(Value) end, Data), ok;
        false -> tuple_foreach(Fun, Data, erlang:tuple_size(Data))
    end.

tuple_foreach(_Fun, _Tuple, 0) -> ok;
tuple_foreach(Fun, Tuple, Index) ->
    Fun(element(Index, Tuple)),
    tuple_foreach(Fun, Tuple, Index-1).


all(known, #coll{rev_indexes = RevIndexes}) ->
    maps:keys(RevIndexes);
all(visible, #coll{indexes = Indexes, data = Data}) ->
    [nth(I, Data) || I <- Indexes].


rand(known, #coll{data = Data}) ->
    case len(Data) of
        0 -> empty;
        L -> nth(rand:uniform(1, L), Data)
    end;
rand(visible, #coll{indexes = []}) -> empty;
rand(visible, #coll{indexes = Indexes, data = Data}) ->
    nth(lists:nth(rand:uniform(1, length(Indexes)), Indexes), Data).
