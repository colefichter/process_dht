-module(dht_util).

% API:
-export([node_id/0, hash/1]).

% Generate a random node ID, returned in the form of a hash. With this format
% we can compare document hashes directly to node IDs to locate documents.
node_id() ->
    %The chance of collisions is small with a very large random integer, but
    %we will use the local node name as a salt to add extra protection.
    Salt = atom_to_list(node()),
    ID = random:uniform(99999999999999999999999999999999999999999999999999),
    hash(Salt ++ integer_to_list(ID)).

hash(Data) ->
    Binary160 = crypto:hash(sha, Data),
    hexstring(Binary160).

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Convert a 160-bit binary hash to a hex string.
hexstring(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X])).

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

node_id_returns_string_test() ->
    ?assert(is_list(node_id())).

hash_returns_string_test() ->
    ?assert(is_list(hash("TESTING TESTING 123!"))).