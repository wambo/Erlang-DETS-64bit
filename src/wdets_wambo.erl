-module(wdets_wambo).

-include("wdets.hrl").

%% For #file_info record
-include_lib("kernel/include/file.hrl").

-export([first/1,
	 next/2,
	 insert/2,
	 limitN/4,
	 limitNv9/4,
	 limit/4,
	 limit2/3,
	 checklimit/1,
	 test/1,
	 test2/1,
	 read/3,
	 readB/3,
	 verify1/1,

	 file_size/1,
	 compareSz/3,
	 compareSz/4,
	 sizeData/4,

	 compareSpeed/4,
	 speedData/5,

	 average/1,

	 ipread_s32bu_p64bu/3
	 ]).

-export([get_id/1]).



get_id(SId) ->
    Id = list_to_atom(SId),
    case get(Id) of
	undefined ->
	    R = ets:new(Id, [ordered_set]),
	    put(Id,R),
	    R;
	R ->
	    R
    end.

first(Name) ->
    Id = get_id(Name),
    Reply = ets:first(Id),
    {ok,Reply}.

insert(Name,EmptyObjects) ->
    io:format("Insert called!!~n"),
    Id = get_id(Name),
    true = ets:insert(Id,EmptyObjects),
    ok.

next(Name,Key) ->
    Id = get_id(Name),
    Reply = ets:next(Id, Key),
    {ok,Reply}.
    
limitN(F,Data,Start,Stop) ->
    [wdets:insert(F,{X,Data}) || X<-lists:seq(Start,Stop-1) ].

limitNv9(F,Data,Start,Stop) ->
    [dets:insert(F,{X,Data}) || X<-lists:seq(Start,Stop-1) ].

limit(_,_,Same,Same) ->
    ok;
limit(N,Data,Start,Stop) ->
    {ok,R} = wdets:open_file(N,[]),
    [begin wdets:insert(R,{X,Data}), erlang:display(X) end || X<-lists:seq(Start,Stop) ],
    wdets:close(R).

limit2(N,Start,Stop) ->
    {ok,R} = wdets:open_file(N,[]),
    [wdets:insert(R,{X,lists:duplicate(200,X)}) || X<-lists:seq(Start,Stop) ],
    wdets:close(R).

checklimit(N) ->
    {ok,R} = wdets:open_file(N,[]),
    check(R,1).

check(R,No) ->
    case wdets:lookup(R,No) of
	[{No,[No|_]}] ->
	    check(R,No+1);
	[] ->
	    ok;
	_Else ->
	    exit(fail)
    end.

read(File,Pos,Sz) ->
    {ok,IO} = file:open(File,[read,binary]), 
    {ok,[<<Bin:Sz/unit:8>>]} = file:pread(IO,[{Pos,Sz}]), 
    file:close(IO),
    Bin.

readB(File,Pos,Sz) ->
    {ok,IO} = file:open(File,[read,binary]), 
    {ok,[<<_Bin:Sz/unit:8>>]=N} = file:pread(IO,[{Pos,Sz}]), 
    file:close(IO),
    N.

test(Fname) ->
    {ok,R} = wdets:open_file(Fname,[]),
    wdets:lookup(R,1),
    wdets:close(R).

test2(Fname) ->
    {ok,R} = wdets:open_file(Fname), 
    wdets:insert(R,{a,b}),
    wdets:close(R).


%%%%%%%%%%%%%%% Verification tests %%%%%%%%%%%%%%%%   

verify1(TabName) ->
    {ok,R} = wdets:open_file(TabName,[]),
    Data = lists:seq(1,10000000),

    erlang:display("TEST: Genererar en 2GB+ fil. Denna fil blir ungefar 2.6gb"),
    [wdets:insert(R,{X,{X,Data}}) || X<-lists:seq(1,40) ],
    
    erlang:display("TEST: Sätter in ett objekt bortanför 2^31 gransen (2gb)"),
    wdets:insert(R,{key1,Data}), 
    erlang:display("TEST: Verifierar att objektet gar att hamta."),
    [{key1,Data}] = wdets:lookup(R, key1),
    erlang:display("TEST: Det fungerade!"),
    
    erlang:display("TEST: Stänger och öppnar om filen, verifierar igen"),
    wdets:close(R),
    {ok,R1} = wdets:open_file(TabName),
    [{key1,Data}] = wdets:lookup(R1, key1),
    erlang:display("TEST: Det fungerade!"),
    
    erlang:display("TEST: Genererar yttligare objekt till filen, filen bor bli 4.6gb"),
    [wdets:insert(R1,{X,{X,Data}}) || X<-lists:seq(41,70) ],

    erlang:display("TEST: Sätter in ett objekt bortanför 2^32 gransen"),
    wdets:insert(R1,{key2,Data}), 
    erlang:display("TEST: Verifierar att om man hämtar den, så får man rätt objekt"),
    [{key2,Data}] = wdets:lookup(R1, key2),
    erlang:display("TEST: Det fungerade!"),
    erlang:display("TEST: Stänger och öppnar om filen, verifierar igen"),
    wdets:close(R1),
    {ok,R2} = wdets:open_file(TabName),
    [{key2,Data}] = wdets:lookup(R2, key2),
    erlang:display("TEST: Det fungerade!"),

    erlang:display("TEST: Tar bort dem 2 test objekten"),
    wdets:delete(R2, key2),
    wdets:delete(R2, key1),
    erlang:display("TEST: Verifierar att båda är borta"),
    [] = wdets:lookup(R2, key2),
    [] = wdets:lookup(R2, key1),
    erlang:display("TEST: Det fungerade!"),
    
    wdets:close(R2),
    erlang:display("TEST: Allt fungerade"),
    ok.
    


%%%%%%%%%%%%%%% Size test code %%%%%%%%%%%%%%%%   

%% Start = number of elements to start with
%% Inc = step size
%% Stop = when elements is equal or above stop, we're done
%% {From,To} = Used to generate data, lists:seq(From,To)
%% T = number of tries for each size, used to provide an average value
%%% The output data will be stored in file named "SizeDataV....",
%%% One for each version. The data inside will be comma seperated "Time,ElementCount\n"
sizeData(Start, Inc, Stop, {From,To}) ->
    LStart = "," ++ integer_to_list(Start) ++ ",",
    LInc = integer_to_list(Inc) ++ ",",
    LStop = integer_to_list(Stop) ++ ",",
    LLength = integer_to_list(From) ++ "-" ++ integer_to_list(To),
    {ok,F1} = file:open("SizeDatav9" ++ LStart ++ LInc ++ LStop ++ LLength++".txt",[write]),
    {ok,F2} = file:open("SizeDatav10" ++ LStart ++ LInc ++ LStop ++ LLength++".txt",[write]),
    timer:sleep(200),
    sizeDataAux({F1,F2}, Start, Inc, Stop, lists:seq(From,To)).

sizeDataAux(_F, Next, _Inc, Stop, _Data) 
  when Next >= Stop ->
    done;
sizeDataAux({F1,F2}=F, Next, Inc, Stop, Data) ->
    timer:sleep(200), %% Await a possible flush
    {V9,V10} = compareSz({a,b}, Data, 0, Next),
    file:write(F1, integer_to_list(V9) ++ "," ++ integer_to_list(Next) ++ "\n"),
    file:write(F2, integer_to_list(V10) ++ "," ++ integer_to_list(Next) ++ "\n"),
    sizeDataAux(F,Next+Inc, Inc, Stop, Data).

compareSz(F, Start, Stop) ->
    D = lists:seq(1,10000),
    compareSz(F, D, Start, Stop).

compareSz({F1,F2}, D, Start, Stop) ->
    {ok,V9} = dets:open_file(F1,[]),
    {ok,V10} = wdets:open_file(F2,[]),
    limitNv9(V9, D, Start, Stop),
    limitN(V10, D, Start, Stop),    
    R = {file_size(F1), file_size(F2)},
    dets:close(F1), wdets:close(F2),
    file:delete(V9), file:delete(V10),
    R.

file_size(F) ->
    {ok,R} = file:read_file_info(F),
    R#file_info.size.




%%%%%%%%%%%%%%% Speed test code %%%%%%%%%%%%%%%%   
%% Start = number of elements to start with
%% Inc = step size
%% Stop = when elements is equal or above stop, we're done
%% {From,To} = Used to generate data, lists:seq(From,To)
%% T = number of tries for each size, used to provide an average value
%%% The output data will be stored in file named "SpeedDataV....txt",
%%% One for each version. The data inside will be comma seperated, such as "Time,ElementCount\n"
speedData(Start, Inc, Stop, {From,To}, T) ->
    LStart = "," ++ integer_to_list(Start) ++ ",",
    LInc = integer_to_list(Inc) ++ ",",
    LStop = integer_to_list(Stop) ++ ",",
    LLength = integer_to_list(From) ++ "-" ++ integer_to_list(To),
    {ok,F1} = file:open("SpeedDatav9" ++ LStart ++ LInc ++ LStop ++ LLength++".txt",[write]),
    {ok,F2} = file:open("SpeedDatav10" ++ LStart ++ LInc ++ LStop ++ LLength++".txt",[write]),
    timer:sleep(200),
    speedDataAux({F1,F2}, Start, Inc, Stop, lists:seq(From,To), T).

speedDataAux(F, Next, Inc, Stop, Data, T) ->
    speedDataAux(F, Next, Inc, Stop, Data, T, 0, {[],[]}).

speedDataAux(_F, Next, _Inc, Stop, _Data, _T, _T1, _Avg) 
  when Next >= Stop ->
    done;
speedDataAux(F, Next, Inc, Stop, Data, T, T1, {AvgV9, AvgV10})
    when T > T1 ->
    timer:sleep(200), %% Await a possible flush
    {V9,V10} = compareSpeed({a,b}, Data, 0, Next),
    speedDataAux(F, Next, Inc, Stop, Data, T, T1+1, {[V9 | AvgV9], [V10 | AvgV10]});
speedDataAux({F1,F2}=F, Next, Inc, Stop, Data, T, _T1, {AvgV9, AvgV10}) ->
    file:write(F1, integer_to_list(round(average(AvgV9))) ++ "," ++ integer_to_list(Next) ++ "\n"),
    file:write(F2, integer_to_list(round(average(AvgV10))) ++ "," ++ integer_to_list(Next) ++ "\n"),
    speedDataAux(F, Next+Inc, Inc, Stop, Data, T, 0, {[], []}).


compareSpeed({F1,F2}, D, Start, Stop) ->
    {ok,V10} = wdets:open_file(F1,[]),
    {ok,V9} = dets:open_file(F2,[]),
    {R1,_} = timer:tc(wdets_wambo, limitN ,[V10, D, Start, Stop]),
    {R2,_} = timer:tc(wdets_wambo, limitNv9, [V9, D, Start, Stop]),
    wdets:close(F1), dets:close(F2),
    file:delete(V10), file:delete(V9),
    {R1,R2}.


average(X) -> sum(X,0) / len(X,0).
	
sum([H|T],X) -> sum(T,X+H);
sum([],X) -> X.
	
len([_|T],X) -> len(T,X+1);
len([],X) -> X.



%%%%%%%%%%%%%%% PROTOTYPE features %%%%%%%%%%%%%%%%   
ipread_s32bu_p64bu(Fptr,Pos,_MaxSize) ->
    {ok,[<<Size:32,Ptr:64>>]} = file:pread(Fptr,[{Pos,(4+8)}]),
    {ok,[Bin]} = file:pread(Fptr,[{Ptr,Size}]),
    {ok,{Size,Ptr,Bin}}.
