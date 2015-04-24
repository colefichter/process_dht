-module(ring).
% A simple process ring made up of a doubly-linked ring of processes.
% A message can be passed in either direction around the ring.
% This module is not used by the process DHT, but illustrates the concept of a
% process ring without all of the complexity of the Chord architecture.
%
% Example usage in the Erlang shell:
%  > c(ring).
%  > First = ring:start(5).
%  > ring:send_anti_cw("Hi", First, 10).
%  > {relay,anti_clockwise,10,1,"Hi"}
%  > <0.38.0> Relaying 1 "Hi"
%  > <0.42.0> Relaying 2 "Hi"
%  > <0.41.0> Relaying 3 "Hi"
%  > <0.40.0> Relaying 4 "Hi"
%  > <0.39.0> Relaying 5 "Hi"
%  > <0.38.0> Relaying 6 "Hi"
%  > <0.42.0> Relaying 7 "Hi"
%  > <0.41.0> Relaying 8 "Hi"
%  > <0.40.0> Relaying 9 "Hi"
%  > <0.39.0> Relaying 10 "Hi"
%  > <0.38.0> Dropping message "Hi" after 10 relays.

-compile([export_all]).

start(NumNodes) ->
    Nodes = [spawn(ring, server_loop, [not_set, not_set]) || _ <- lists:seq(1, NumNodes)],
    [First|_] = Nodes,
    [Last|_] = lists:reverse(Nodes),
    ok = link_all(Nodes),
    link(Last, First),
    First.

% Sends a message clockwise around the ring indefinitely
send_cw(Message, StartNode, TotalRelays) -> StartNode ! {relay, clockwise, TotalRelays, 1, Message}.

% Sends a message anti-clockwise around the ring indefinitely
send_anti_cw(Message, StartNode, TotalRelays) -> StartNode ! {relay, anti_clockwise, TotalRelays, 1, Message}.

link_all([]) -> ok;
link_all([_Last]) -> ok;
link_all([One|[Two|T]]) ->    
    link(One, Two),
    link_all([Two|T]).

link(NodeA, NodeB) ->
    io:format("Linking ~p to ~p~n", [NodeA, NodeB]),
    NodeA ! {set_next, NodeB},
    NodeB ! {set_prev, NodeA}.

server_loop(Prev, Next) ->
    receive
        {set_prev, NewPrev} -> server_loop(NewPrev, Next);
        {set_next, NewNext} -> server_loop(Prev, NewNext);
        {relay, _Direction, TotalRelays, NumRelays, Message} when NumRelays > TotalRelays ->
            io:format("~p Dropping message ~p after ~p relays.~n", [self(), Message, TotalRelays]),
            server_loop(Prev, Next);
        {relay, Direction, TotalRelays, NumRelays, Message} ->
            timer:sleep(500), % Some delay helps us see what is actually happening.
            io:format("~p Relaying ~p ~p~n", [self(), NumRelays, Message]),            
            Node = case Direction of
                clockwise -> Next;
                anti_clockwise -> Prev
            end,
            Node ! {relay, Direction, TotalRelays, NumRelays + 1, Message},
            server_loop(Prev, Next);
        {stop} -> exit(normal)
    end.