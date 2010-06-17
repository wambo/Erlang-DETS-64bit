-module(wdets_wambo).

-include("wdets.hrl").

-export([first/1,
	 next/2,
	 insert/2,
	 limit/4,
	 limit2/3,
	 checklimit/1,
	 test/1,
	 test2/1,
	 read/3,
	 readB/3,
	 read_32_64/3
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
    
limit(N,Data,Start,Stop) ->
    {ok,R} = wdets:open_file(N,[]),
    [wdets:insert(R,{X,Data}) || X<-lists:seq(Start,Stop) ],
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
    {ok,[<<Bin:Sz/unit:8>>]=N} = file:pread(IO,[{Pos,Sz}]), 
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

read_32_64(Fptr,Pos,_MaxSize) ->
    {ok,[<<Size:32,Ptr:64>>]} = file:pread(Fptr,[{Pos,(4+8)}]),
    {ok,[Bin]} = file:pread(Fptr,[{Ptr,Size}]),
    {ok,{Size,Ptr,Bin}}.
