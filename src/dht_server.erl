-module(dht_server).
-behaviour(gen_server).

% Client API:
-export([start_link/0, store/1, fetch/1]).

% Server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

%---------------------------------------------------------------------------------------------
% Client API
%---------------------------------------------------------------------------------------------
start_link() ->
    {ok, Pid} = gen_server:start_link(?MODULE, [], []),
    Pid.

store(Value) ->
    Key = dht_util:hash(Value),
    store(Key, Value).

% Get a value from the DHT by its key, regardless of which node is holding the value.
fetch(Key) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:call(ServerPid, {fetch, Key}).

%---------------------------------------------------------------------------------------------
% Internal API for talking to the servers
%---------------------------------------------------------------------------------------------
join(Pid) -> gen_server:cast(Pid, join).

set_next(Pid, Id) -> gen_server:cast(Pid, {set_next, Id, self()}).

% Consistent hashing in a DHT means we hash before sending the query to the server.
% In fact, the server will be chosen based on the Key hash. Having store/2 also allows
% for easy unit testing.
store(Key, Value) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:cast(ServerPid, {store, Key, Value}),
    Key.

% Locate the {ServerId, ServerPid} responsible for storing the given Key hash.
key_lookup(Key) ->
    gen_server:cast(whereis(dht_root_node), {key_lookup, Key, self()}),
    await_reply(). % The response might not be from dht_root_node!

% Find the {Id, Pid} details for the next and previous servers of a given Id hash.
find_neighbours(Id) ->
    gen_server:cast(whereis(dht_root_node), {find_neighbours, Id, self()}),
    await_reply(). % The response might not be from dht_root_node!

stop(Pid) -> gen_server:cast(Pid, stop).

%---------------------------------------------------------------------------------------------
% Server implementation
%---------------------------------------------------------------------------------------------
init(testing) -> % For testing only.
    Id = dht_util:node_id(),
    State = {dict:new(), Id, {Id, self()}},
    {ok, State};
init([]) ->
    ensure_root_node_is_registered(),
    Id = dht_util:node_id(),
    State = {dict:new(), Id, {Id, self()}},
    {ok, State}.

handle_cast({store, Key, Value}, {Dict, Id, Next}) ->
    NewDict = dict:store(Key, Value, Dict),
    {noreply, {NewDict, Id, Next}};
handle_cast(join, {Dict, Id, _Next}) ->
    %Figure out who are my neighbors and adjust the ring to insert myself in between them.
    {_PrevId, PrevPid, NextId, NextPid} = find_neighbours(Id),
    set_next(PrevPid, Id), %Tell previous node to point at me
    {noreply, {Dict, Id, {NextId, NextPid}}}; %I'll now point at previous node's old Next node.
handle_cast({set_next, NextId, NextPid}, {Dict, Id, _Next}) -> {noreply, {Dict, Id, {NextId, NextPid}}};
handle_cast({key_lookup, Key, RequestingPid}, State = {_Dict, Id, Next}) ->
    handle_key_lookup(RequestingPid, Key, Id, Next),
    {noreply, State};
handle_cast({find_neighbours, HashId, RequestingPid}, State = {_Dict, Id, Next}) ->
    handle_find_neighbours(RequestingPid, HashId, Id, Next),
    {noreply, State};
%This is only required for testing.
handle_cast({set_state, NewId, NextId, NextPid}, _State) -> {noreply, {dict:new(), NewId, {NextId, NextPid}}};
handle_cast(stop, State) -> {stop, normal, State}.

handle_call({fetch, Key}, _From, State = {Dict, _, _}) ->
    Reply = case dict:find(Key, Dict) of
        error -> {not_found, Key};
        Result -> Result
    end,
    {reply, Reply, State}.

%---------------------------------------------------------------------------------------------
% Unused gen_server callbacks
%---------------------------------------------------------------------------------------------
handle_info(_Any, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%---------------------------------------------------------------------------------------------
% Helpers and utilities
%---------------------------------------------------------------------------------------------
ensure_root_node_is_registered() -> ensure_root_node_is_registered(whereis(dht_root_node)).
% This is the first node in the ring. Register it.
ensure_root_node_is_registered(undefined) -> register(dht_root_node, self());
% The ring exists. Send myself a message to join the ring after initialization completes.
ensure_root_node_is_registered(RootPid) when is_pid(RootPid) -> join(self()).

% This allows us to get a response from a different server than the one we sent the request to!
await_reply() ->
    receive
        Reply -> Reply
    after 1000 -> {error, request_timed_out}
    end.

% There is only one node in the ring right now
handle_find_neighbours(From, _HashId, ServerId, {ServerId, ServerPid}) ->
    From ! {ServerId, ServerPid, ServerId, ServerPid};
% Handle wrap-around of the ring...
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId -> 
    case (HashId > ServerId orelse HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> gen_server:cast(NextPid, {find_neighbours, HashId, From})
    end;
% Handle all the other nodes that do not wrap around.
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> gen_server:cast(NextPid, {find_neighbours, HashId, From})
    end.

% The hash is the same as the server ID, or there is only one server in the ring.
handle_key_lookup(From, HashId, ServerId, {NextId, _NextPid}) 
    when HashId == ServerId; ServerId == NextId ->
    From ! {ServerId, self()};
% Handle wrap-around of the ring...
handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId ->
    case (HashId > ServerId orelse HashId =< NextId) of
        true -> From ! {NextId, NextPid};
        false -> gen_server:cast(NextPid, {key_lookup, HashId, From})
    end;
handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {NextId, NextPid};
        false -> gen_server:cast(NextPid, {key_lookup, HashId, From})
    end.

%---------------------------------------------------------------------------------------------
% Unit tests
%---------------------------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

start_first_node_test() ->
    ?assertEqual(undefined, whereis(dht_root_node)),
    Pid = start_link(),
    ?assertEqual(Pid, whereis(dht_root_node)),
    unregister(dht_root_node).

find_neighbours_singlenode_test() ->
    N0 = setup_test_singlenode(),
    ?assertEqual({0, N0, 0, N0}, find_neighbours(1)),
    ?assertEqual({0, N0, 0, N0}, find_neighbours(10)),
    unregister(dht_root_node).

find_neighbours_multinode_test() -> helper_find_neighbours_test(false).

find_neighbours_multinode_outoforder_test() -> helper_find_neighbours_test(true).

key_lookup_singlenode_test() ->
    N0 = setup_test_singlenode(),
    % Since we haven't stored any values yet, we're only testing the node lookup function here.
    ?assertEqual({0, N0}, key_lookup(0)),
    ?assertEqual({0, N0}, key_lookup(1)),
    ?assertEqual({0, N0}, key_lookup(10)),
    unregister(dht_root_node).

key_lookup_multinode_test() -> helper_key_lookup_test(false).

key_lookup_multinode_outoforder_test() -> helper_key_lookup_test(true).

% Test the whole system, end-to-end, with real IDs and hashes (rather than integers).
system_test() ->
    DisplayDebugTrace = true, % Set this to true to turn on tracing
    S1 = start_link(),
    timer:sleep(50),
    S2 = start_link(),
    timer:sleep(50),
    S3 = start_link(),
    timer:sleep(50),
    S4 = start_link(),
    timer:sleep(50),
    [sys:trace(X, DisplayDebugTrace) || X <- [S1, S2, S3, S4]],
    {not_found, _} = fetch("not a key"),
    K = store("Cole is the best"),
    io:format("K = ~p~n", [K]),
    {ok, "Cole is the best"} = fetch(K),
    {not_found, _} = fetch("adsfasfd"),
    {ok, "Cole is the best"} = fetch(K),
    [stop(X) || X <- [S1, S2, S3, S4]],
    done.

%---------------------------------------------------------------------------------------------
% Helper functions only used by unit tests:
%---------------------------------------------------------------------------------------------
start_link_test() -> % Dependency injection for testing.
    {ok, Pid} = gen_server:start_link(?MODULE, testing, []),
    Pid.

setup_test_singlenode() ->
    N0 = start_link_test(),
    register(dht_root_node, N0),
    gen_server:cast(N0, {set_state, 0, 0, N0}),
    timer:sleep(100), % wait for message passing to occur.
    N0.

setup_test_ring() ->
    [N1, N6, N10] = [start_link_test() || _X <- [1, 6, 10]],
    gen_server:cast(N1, {set_state, 1, 6, N6}),
    gen_server:cast(N6, {set_state, 6, 10, N10}),
    gen_server:cast(N10, {set_state, 10, 1, N1}),
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
    % print(),
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
    unregister(dht_root_node).