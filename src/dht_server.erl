-module(dht_server).

% Client API:
-export([start_node/0, store/1, fetch/1]).

start_node() ->
    start_node(whereis(dht_root_node)).

store(Value) ->
    Key = dht_util:hash(Value),
    store(Key, Value).

% Consistent hashing in a DHT means we hash before sending the query to the server.
% In fact, the server will be chosen based on the Key hash. Having store/2 also allows
% for easy unit testing.
store(Key, Value) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    ServerPid ! {self(), {store, Key, Value}},
    Key.

% Get a value from the DHT by its key, regardless of which node is holding the value.
fetch(Key) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    rpc(ServerPid, {fetch, Key}).

%%%===================================================================
%%% Internal API for talking to the server(s)
%%%===================================================================

% Locate the {ServerId, ServerPid} responsible for storing the given Key hash.
key_lookup(Key) -> rpc({key_lookup, Key}).

% Find the {Id, Pid} details for the next and previous servers of a given Id hash.
find_neighbours(Id) -> rpc({find_neighbours, Id}).

print() -> % Print some debugging information to the shell.
    ServerPid = whereis(dht_root_node),
    ServerPid ! {print, ServerPid}.

%%%===================================================================
%%% Server Implementation
%%%===================================================================
server_loop(Dict, Id, Next) ->
    receive
        {_From, {store, Key, Value}} ->
            NewDict = dict:store(Key, Value, Dict),
            server_loop(NewDict, Id, Next);
        {From, {fetch, Key}} ->
            From ! dict:find(Key, Dict),
            server_loop(Dict, Id, Next);
        {From, {key_lookup, Key}} -> 
            handle_key_lookup(From, Key, Id, Next),
            server_loop(Dict, Id, Next);
        {From, {find_neighbours, HashId}} -> 
            handle_find_neighbours(From, HashId, Id, Next),
            server_loop(Dict, Id, Next);
        {join} ->
            %Figure out who are my neighbors and adjust the ring to insert myself in between them.
            {_PrevId, PrevPid, NextId, NextPid} = find_neighbours(Id),
            PrevPid ! {set_next, Id, self()}, %Tell previous node to point at me
            server_loop(Dict, Id, {NextId, NextPid}); %I'll now point at previous node's old next node.
        {set_next, NextId, NextPid} -> server_loop(Dict, Id, {NextId, NextPid});
        % The ones after this are mostly for testing:
        {print, RequestInvokedAtServerPid} ->
            handle_print(RequestInvokedAtServerPid, Next),
            server_loop(Dict, Id, Next);
        {set_state, NewId, NextId, NextPid} -> server_loop(dict:new(), NewId, {NextId, NextPid})
    end.


% There is only one node in the ring right now
handle_find_neighbours(From, _HashId, ServerId, {ServerId, ServerPid}) ->
    From ! {ServerId, ServerPid, ServerId, ServerPid};

% Handle wrap-around of the ring...
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId -> 
    case (HashId > ServerId orelse HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> NextPid ! {From, {find_neighbours, HashId}}
    end;

% Handle all the other nodes that do not wrap around.
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> NextPid ! {From, {find_neighbours, HashId}}
    end.


% The hash is the same as the server ID, or there is only one server in the ring.
handle_key_lookup(From, HashId, ServerId, {NextId, _NextPid}) 
    when HashId == ServerId; ServerId == NextId ->
    From ! {ServerId, self()};

% Handle wrap-around of the ring...
handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId ->
    case (HashId > ServerId orelse HashId =< NextId) of
        true -> From ! {NextId, NextPid};
        false -> NextPid ! {From, {key_lookup, HashId}}
    end;

handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {NextId, NextPid};
        false -> NextPid ! {From, {key_lookup, HashId}}
    end.


handle_print(RequestInvokedAtServerPid, {_NextId, NextPid}) ->
    case RequestInvokedAtServerPid =/= NextPid of %forward the message once around the ring.
        true -> NextPid ! {print, RequestInvokedAtServerPid};
        false -> ok
    end.

%%%===================================================================
%%% Internal helpers
%%%===================================================================

start_node(undefined) -> % This is the first node in the ring.
    Id = dht_util:node_id(),
    Pid = spawn(fun() -> server_loop(dict:new(), Id, {Id, self()}) end),
    true = register(dht_root_node, Pid),
    Pid;
start_node(RootPid) when is_pid(RootPid) ->
    Id = dht_util:node_id(),
    Pid = spawn(fun() -> server_loop(dict:new(), Id, {Id, self()}) end),
    Pid ! {join},
    Pid.

rpc(Message) -> rpc(whereis(dht_root_node), Message).

rpc(Server, Message) ->
    Server ! {self(), Message},
    receive
        Reply -> Reply
    end.

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

start_first_node_test() ->
    ?assertEqual(undefined, whereis(dht_root_node)),
    Pid = start_node(),
    ?assertEqual(Pid, whereis(dht_root_node)),
    unregister(dht_root_node).

find_neighbours_singlenode_test() ->
    N0 = setup_test_singlenode(),
    ?assertEqual({0, N0, 0, N0}, find_neighbours(1)),
    ?assertEqual({0, N0, 0, N0}, find_neighbours(10)),
    print(),
    unregister(dht_root_node).

find_neighbours_multinode_test() -> helper_find_neighbours_test(false).

find_neighbours_multinode_outoforder_test() -> helper_find_neighbours_test(true).

key_lookup_singlenode_test() ->
    N0 = setup_test_singlenode(),
    % Since we haven't stored any values yet, we're only testing the node lookup function here.
    ?assertEqual({0, N0}, key_lookup(0)),
    ?assertEqual({0, N0}, key_lookup(1)),
    ?assertEqual({0, N0}, key_lookup(10)),
    print(),
    unregister(dht_root_node).

key_lookup_multinode_test() -> helper_key_lookup_test(false).

key_lookup_multinode_outoforder_test() -> helper_key_lookup_test(true).

%%%===================================================================
%%% Helper functions only used by unit tests:
%%%===================================================================

setup_test_singlenode() ->
    N0 = spawn(fun() -> server_loop(dict:new(), 0, nothing) end),
    register(dht_root_node, N0),
    N0 ! {set_state, 0, 0, N0},
    timer:sleep(100), % wait for message passing to occur.
    N0.

setup_test_ring() ->
    [N1, N6, N10] = [spawn(fun() -> server_loop(dict:new(), X, nothing) end) || X <- [1, 6, 10]],
    N1 ! {set_state, 1, 6, N6},
    N6 ! {set_state, 6, 10, N10},
    N10 ! {set_state, 10, 1, N1},
    timer:sleep(100), % wait for message passing to occur.
    [N1, N6, N10].

helper_find_neighbours_test(OutOfOrder) ->
    %Create a ring with nodes N1, N6, N10 (the number being the id).
    [N1, N6, N10] = setup_test_ring(),
    case OutOfOrder of
        true -> register(dht_root_node, N10);
        false -> register(dht_root_node, N1)
    end,
    ?assertEqual({10, N10, 1, N1}, find_neighbours(0)),
    ?assertEqual({1, N1, 6, N6}, find_neighbours(2)),
    ?assertEqual({1, N1, 6, N6}, find_neighbours(3)),
    ?assertEqual({1, N1, 6, N6}, find_neighbours(4)),
    ?assertEqual({1, N1, 6, N6}, find_neighbours(5)),
    ?assertEqual({6, N6, 10, N10}, find_neighbours(7)),
    ?assertEqual({6, N6, 10, N10}, find_neighbours(8)),
    ?assertEqual({6, N6, 10, N10}, find_neighbours(9)),
    ?assertEqual({10, N10, 1, N1}, find_neighbours(11)),
    print(),
    unregister(dht_root_node).

helper_key_lookup_test(OutOfOrder) ->
    %Create a ring with nodes N0, N6, N10 (the number being the id).
    [N1, N6, N10] = setup_test_ring(),
    case OutOfOrder of
        true -> register(dht_root_node, N6);
        false -> register(dht_root_node, N1)
    end,
    % Since we haven't stored any values yet, we're only testing the node lookup function here.
    ?assertEqual({1, N1}, key_lookup(0)),
    ?assertEqual({1, N1}, key_lookup(1)),
    ?assertEqual({6, N6}, key_lookup(2)),
    ?assertEqual({6, N6}, key_lookup(3)),
    ?assertEqual({6, N6}, key_lookup(4)),
    ?assertEqual({6, N6}, key_lookup(5)),
    ?assertEqual({6, N6}, key_lookup(6)),
    ?assertEqual({10, N10}, key_lookup(7)),
    ?assertEqual({10, N10}, key_lookup(8)),
    ?assertEqual({10, N10}, key_lookup(9)),
    ?assertEqual({10, N10}, key_lookup(10)),
    ?assertEqual({1, N1}, key_lookup(11)),
    print(),
    unregister(dht_root_node).