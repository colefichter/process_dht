-module(dht_server2).
-behaviour(gen_server).

% Client API:
-export([start_link/0, store/1, fetch/1]).%, print/0]).

% Server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

%---------------------------------------------------------------------------------------------
% Client API
%---------------------------------------------------------------------------------------------
start_link() ->
    {ok, Pid} = gen_server:start_link(?MODULE, [], []),
    Pid.

start_link_test() -> % Dependency injection for testing.
    {ok, Pid} = gen_server:start_link(?MODULE, testing, []),
    Pid.

store(Value) ->
    Key = dht_util:hash(Value),
    store(Key, Value).

% Consistent hashing in a DHT means we hash before sending the query to the server.
% In fact, the server will be chosen based on the Key hash. Having store/2 also allows
% for easy unit testing.
store(Key, Value) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:cast(ServerPid, {self(), {store, Key, Value}}),
    Key.

% Get a value from the DHT by its key, regardless of which node is holding the value.
fetch(Key) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:call(ServerPid, {fetch, Key}).

% % Print some debugging information to the shell.
% print() ->
%     ServerPid = whereis(dht_root_node),
%     gen_server:cast(ServerPid, {print, ServerPid}).

%---------------------------------------------------------------------------------------------
% Internal API for talking to the servers
%---------------------------------------------------------------------------------------------
join(Pid) -> gen_server:cast(Pid, join).
set_next(Pid, Id) -> gen_server:cast(Pid, {set_next, Id, self()}).

% Locate the {ServerId, ServerPid} responsible for storing the given Key hash.
key_lookup(Key) -> 
    gen_server:cast(whereis(dht_root_node), {key_lookup, Key, self()}),
    await().

% Find the {Id, Pid} details for the next and previous servers of a given Id hash.
find_neighbours(Id) -> 
    gen_server:cast(whereis(dht_root_node), {find_neighbours, Id, self()}),
    await().

%---------------------------------------------------------------------------------------------
% Server implementation
%---------------------------------------------------------------------------------------------
% For testing...
init(testing) -> 
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
    {noreply, {Dict, Id, {NextId, NextPid}}}; %I'll now point at previous node's old next node.
handle_cast({set_next, NextId, NextPid}, {Dict, Id, _Next}) -> {noreply, {Dict, Id,{NextId, NextPid}}};
% handle_cast({print, RequestInvokedAtServerPid}, {Dict, Id, Next}) -> 
%     handle_print(RequestInvokedAtServerPid, Next),
%     {noreply, {Dict, Id, Next}};
handle_cast({key_lookup, Key, RequestingPid}, {Dict, Id, Next}) -> 
    handle_key_lookup(RequestingPid, Key, Id, Next),
    {noreply, {Dict, Id, Next}};
handle_cast({find_neighbours, HashId, RequestingPid}, {Dict, Id, Next}) ->
    handle_find_neighbours(RequestingPid, HashId, Id, Next),
    {noreply, {Dict, Id, Next}};
%This is only required for testing.
handle_cast({set_state, NewId, NextId, NextPid}, _State) -> {noreply, {dict:new(), NewId, {NextId, NextPid}}}.

handle_call({fetch, Key}, _From, {Dict, Id, Next}) -> 
    Reply = dict:find(Key, Dict),
    {reply, Reply, {Dict, Id, Next}}.

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
ensure_root_node_is_registered(undefined) -> 
    true = register(dht_root_node, self()),
    ok;
% The ring exists. Send myself a message to join the ring after initialization completes.
ensure_root_node_is_registered(RootPid) when is_pid(RootPid) -> 
    join(self()),
    ok.

await() -> receive AnyMessage -> AnyMessage end.

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


% handle_print(RequestInvokedAtServerPid, {_NextId, NextPid}) ->
%     case RequestInvokedAtServerPid =/= NextPid of %forward the message once around the ring.
%         true -> gen_server:cast(NextPid, {print, RequestInvokedAtServerPid});
%         false -> ok
%     end.

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
    %print(),
    unregister(dht_root_node).

find_neighbours_multinode_test() -> helper_find_neighbours_test(false).

find_neighbours_multinode_outoforder_test() -> helper_find_neighbours_test(true).

key_lookup_singlenode_test() ->
    N0 = setup_test_singlenode(),
    % Since we haven't stored any values yet, we're only testing the node lookup function here.
    ?assertEqual({0, N0}, key_lookup(0)),
    ?assertEqual({0, N0}, key_lookup(1)),
    ?assertEqual({0, N0}, key_lookup(10)),
    % print(),
    unregister(dht_root_node).

key_lookup_multinode_test() -> helper_key_lookup_test(false).

key_lookup_multinode_outoforder_test() -> helper_key_lookup_test(true).

%---------------------------------------------------------------------------------------------
% Helper functions only used by unit tests:
%---------------------------------------------------------------------------------------------
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
    % print(),
    unregister(dht_root_node).