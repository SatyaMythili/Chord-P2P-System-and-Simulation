-module(chord).
-export([create_network/2, node/4, listen_task_completion/2, main/2]).


randomNode(Node_id, []) -> Node_id;
randomNode(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).

% isprocess alive needs to be changed


% get_successor_from_state(State) ->
%     Successor = lists:nth(1, dict:fetch(finger_table, State)),
%     case is_process_alive(Successor) of
%         true -> Successor;
%         _ -> dict:fetch(id , State)
%     end
% .


% get_predecessor(Id) ->
%     case ets:lookup(tbl, Id) of
%         [{_, P}] -> P;
%         [] -> nil
%     end
% .

% is_in_range(From, Key, To, M) ->
%     case To > trunc(math:pow(2, M)) + 1 of
%         true -> UpdatedTo = 1;
%         _ -> UpdatedTo = To
%     end,

%     if 
%         From < UpdatedTo -> (From < Key) and (Key < UpdatedTo);
%         From == UpdatedTo -> Key =/= From;
%         From > UpdatedTo -> ((Key > 0) and (Key < UpdatedTo)) or ((Key > From) and (Key =< trunc(math:pow(2, M))))
%     end
% .


% notify_succesor_to_update_predecessor(Successor, NewPredecessor, State) ->
%     get_node_pid(Successor, dict:fetch(networkstate, State)) ! {notify, NewPredecessor}
% .

% replacenth(L,Index,NewValue) -> 
%  {L1,[_|L2]} = lists:split(Index-1,L),
%  L1++[NewValue|L2].


% update_successor(Successor, State, Index) ->
%     Ft = replacenth(dict:fetch(finger_table, State), Index, Successor),
%     UpdatedState = dict:store(finger_table, Ft, State),
%     UpdatedState
% .

% stabilize(State) -> 
%     Successor = get_successor_from_state(State),
%     case dict:fetch(id, State) == Successor of
%         true -> Predecessor =  dict:fetch(predecessor, State);
%         _ -> Predecessor = get_predecessor(Successor)
%     end,

%     case Predecessor =/= nil and  is_in_range(dict:fetch(id, State), Predecessor, Successor, dict:fetch(m, State)) of
%         true -> UpdatedSuccessor = Predecessor;
%         _ -> UpdatedSuccessor = Successor
%     end,

%     notify_succesor_to_update_predecessor(UpdatedSuccessor, dict:fetch(id, State), State),
%     UpdatedState = update_successor(UpdatedSuccessor, State, 0),
%     UpdatedState
% .

% get_valid_next_id(Next, State) ->
%     NumNodes = trunc(math:pow(2, dict:fetch(State, m))),
%     NextId = dict:fetch(id, State) + trunc(math:pow(2, Next)),
%     if 
%         NextId > NumNodes ->
%             NextId - NumNodes;
%         true -> NextId
%     end
% .

% is_alive(Id, State) ->
%     case get_node_pid(Id, dict:fetch(networkstate, State)) of
%         nil -> false;
%         _ -> true
%     end
% .


% default(Key, DefaultValue, State) ->
%     if
%         Key == nil ->  DefaultValue;
%         true ->
%             case (is_alive(Key, State)) of
%                 true -> Key;
%                 _ -> DefaultValue
%             end
%     end
% .


% closestTo(Node_id, State) ->
%     Range = lists:seq(dict:fetch(m, State) - 1, 0, -1),

%     FingerList = lists:map(fun(i) -> lists:nth(i + 1, dict:fetch(finger_table, State)) end, Range),
%     FilteredList = lists:filtermap(fun(i) -> is_in_range(dict:fetch(id, State), i, Node_id, dict:fetch(m, State)) end, FingerList),

%     case length(FilteredList) > 0 of
%         true -> default(lists:nth(1, FilteredList), dict:fetch(id, State), State);
%         _ -> dict:fetch(id, State)
%     end
% .

% getSuccessorOfClosest(ClosestId, Id, Node_id, HopsCount, Origin) do
%     if 

% find_successor(Origin, Node_id, HopsCount, State) ->
%     Successor = get_successor_from_state(State),
%     case is_in_range(dict:fetch(id), Node_id, Successor + 1, dict:fetch(m, State)) of
%         true -> [Successor, HopsCount];
%         _ -> 
%             ClosestId =  closestTo(Node_id, State),


% .


% fix_fingers(State, Origin) ->
%     NextId = get_valid_next_id(dict:fetch(next, State), State),

%     [Successor, _] = find_successor(Origin, NextId, 0, State),

%     UpdatedState = update_successor(Successor, State, dict:fetch(next, State)),
    
%     dict:store(next, (dict:fetch(next, UpdatedState) + 1) rem dict:fetch(m ,UpdatedState))
% .

% start_stabilizing(State) ->
%     io:fwrite("Starting to stabilize"),
%     StateAfterStabilizing = stabilize(state),
%     StateAfterFingersFix = fix_fingers(StateAfterStabilizing, dict:fetch(id, State))
    
% .

get_forward_distance(Key, Key, _, Distance) ->
    Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
    get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1)
.

get_closest(_, [], MinNode, _, _) ->
    MinNode;
get_closest(Key, FingerNodeIds, MinNode, MinVal, State) ->
    [First| Rest] = FingerNodeIds,
    Distance = get_forward_distance(Key, First, dict:fetch(m, State), 0),
    if
        Distance < MinVal ->
            get_closest(Key, Rest, First, Distance, State);
        true -> 
            get_closest(Key, Rest, MinNode, MinVal, State)
    end
.

get_closest_node(Key, FingerNodeIds, State) ->
    case lists:member(Key, FingerNodeIds) of
        true -> Key;
        _ -> get_closest(Key, FingerNodeIds, -1, 10000000, State)
    end

.

node_listen(NodeState) ->
    Hash = dict:fetch(id, NodeState),
    receive
        {finger_table, FingerTable} -> 
            % io:format("Received Finger for ~p ~p", [Hash, FingerTable]),
            UpdatedState = dict:store(finger_table, FingerTable, NodeState);
            
        {lookup, Id, Key, HopsCount, Pid} ->

                NodeVal = get_closest_node(Key, dict:fetch_keys(dict:fetch(finger_table ,NodeState)), NodeState),
                UpdatedState = NodeState,
                %io:format("Lookup::: ~p  For Key ~p  ClosestNode ~p ~n", [Hash, Key, NodeVal]),
                if 
                    
                    (Hash == Key) -> 
                        taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
                    (NodeVal == Key) and (Hash =/= Key) -> 
                        taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
                    
                    true ->
                        dict:fetch(NodeVal, dict:fetch(finger_table, NodeState)) ! {lookup, Id, Key, HopsCount + 1, self()}
                end
                ;
        {kill} ->
            UpdatedState = NodeState,
            exit("received exit signal");
        {state, Pid} -> Pid ! NodeState,
                        UpdatedState = NodeState;
        {stabilize, NetworkState} -> io:fwrite("Stabilzing the network"),
                                UpdatedState = NodeState
    end, 
    node_listen(UpdatedState).

node(Hash, M, ChordNodes, NodeState) -> 
    io:format("Node is spawned with hash ~p",[Hash]),
    FingerTable = lists:duplicate(M, randomNode(Hash, ChordNodes)),
    NodeStateUpdated = dict:from_list([{id, Hash}, {predecessor, nil}, {finger_table, FingerTable}, {next, 0}, {m, M}]),
    node_listen(NodeStateUpdated)        
.


get_m(NumNodes) ->
    trunc(math:ceil(math:log2(NumNodes)))
.

get_node_pid(Hash, NetworkState) -> 
    case dict:find(Hash, NetworkState) of
        error -> nil;
        _ -> dict:fetch(Hash, NetworkState)
    end
.

add_node_to_chord(ChordNodes, TotalNodes, M, NetworkState) ->
    RemainingHashes = lists:seq(0, TotalNodes - 1, 1) -- ChordNodes,
    Hash = lists:nth(rand:uniform(length(RemainingHashes)), RemainingHashes),
    Pid = spawn(chord, node, [Hash, M, ChordNodes, dict:new()]),
    io:format("~n ~p ~p ~n", [Hash, Pid]),
    [Hash, dict:store(Hash, Pid, NetworkState)]
.


listen_task_completion(0, HopsCount) ->
    mainprocess ! {totalhops, HopsCount}
;

listen_task_completion(NumRequests, HopsCount) ->
    receive 
        {completed, Pid, HopsCountForTask, Key} ->
            % io:format("received completion from ~p, Number of Hops ~p, For Key ~p", [Pid, HopsCountForTask, Key]),
            listen_task_completion(NumRequests - 1, HopsCount + HopsCountForTask)
    end
.

send_message_to_node(_, [], _) ->
    ok;
send_message_to_node(Key, ChordNodes, NetworkState) ->
    [First | Rest] = ChordNodes,
    Pid = get_node_pid(First, NetworkState),
    Pid ! {lookup, First, Key, 0, self()},
    send_message_to_node(Key, Rest, NetworkState)
.


send_messages_all_nodes(_, 0, _, _) ->
    ok;
send_messages_all_nodes(ChordNodes, NumRequest, M, NetworkState) ->
    timer:sleep(1000),
    Key = lists:nth(rand:uniform(length(ChordNodes)), ChordNodes),
    send_message_to_node(Key, ChordNodes, NetworkState),
    send_messages_all_nodes(ChordNodes, NumRequest - 1, M, NetworkState)
.

kill_all_nodes([], _) ->
    ok;
kill_all_nodes(ChordNodes, NetworkState) -> 
    [First | Rest] = ChordNodes,
    get_node_pid(First, NetworkState) ! {kill},
    kill_all_nodes(Rest, NetworkState).

getTotalHops() ->
    receive
        {totalhops, HopsCount} ->
            HopsCount
        end.


send_messages_and_kill(ChordNodes, NumNodes, NumRequest, M, NetworkState) ->
    register(taskcompletionmonitor, spawn(chord, listen_task_completion, [NumNodes * NumRequest, 0])),

    send_messages_all_nodes(ChordNodes, NumRequest, M, NetworkState),

    TotalHops = getTotalHops(),

    io:format("~n Average Hops = ~p ~p ~p ~n", [TotalHops/(NumNodes * NumRequest), TotalHops, NumNodes * NumRequest]),
    kill_all_nodes(ChordNodes, NetworkState)
.

stabilize(ChordNodes, NetworkState) ->
    Pid = get_node_pid(lists:nth(rand:uniform(length(ChordNodes)), ChordNodes), NetworkState),

    case Pid of
        nil -> stabilize(ChordNodes, NetworkState);
        _ -> Pid ! {stabilize, NetworkState}
    end,

    io:fwrite("Stabilizing the network")
.


create_nodes(ChordNodes, _, _, 0, NetworkState) -> 
    [ChordNodes, NetworkState];
create_nodes(ChordNodes, TotalNodes, M, NumNodes, NetworkState) ->
    [Hash, NewNetworkState] = add_node_to_chord(ChordNodes, TotalNodes,  M, NetworkState),
    create_nodes(lists:append(ChordNodes, [Hash]), TotalNodes, M, NumNodes - 1, NewNetworkState)
.


get_ith_successor(_, _, I , I, CurID, M) ->
    CurID;

get_ith_successor(Hash, NetworkState, I, Cur, CurID, M) -> 
    case dict:find((CurID + 1) rem trunc(math:pow(2, M)), NetworkState) of
        error ->
             get_ith_successor(Hash, NetworkState, I, Cur, (CurID + 1) rem trunc(math:pow(2, M)),M);
        _ -> get_ith_successor(Hash, NetworkState, I, Cur + 1, (CurID + 1) rem trunc(math:pow(2, M)),M)
    end
.

get_finger_table(_, _, M, M,FingerList) ->
    FingerList;
get_finger_table(Node, NetworkState, M, I, FingerList) ->
    Hash = element(1, Node),
    Ith_succesor = get_ith_successor(Hash, NetworkState, trunc(math:pow(2, I)), 0, Hash, M),
    get_finger_table(Node, NetworkState, M, I + 1, FingerList ++ [{Ith_succesor, dict:fetch(Ith_succesor, NetworkState)}] )
.


collectfingertables(_, [], FTDict,_) ->
    FTDict;

collectfingertables(NetworkState, NetList, FTDict,M) ->
    [First | Rest] = NetList,
    FingerTables = get_finger_table(First, NetworkState,M, 0,[]),
    collectfingertables(NetworkState, Rest, dict:store(element(1, First), FingerTables, FTDict), M)
.



send_finger_tables_nodes([], _, _) ->
    ok;
send_finger_tables_nodes(NodesToSend, NetworkState, FingerTables) ->
    [First|Rest] = NodesToSend,
    Pid = dict:fetch(First ,NetworkState),
    Pid ! {finger_table, dict:from_list(dict:fetch(First, FingerTables))},
    send_finger_tables_nodes(Rest, NetworkState, FingerTables)
.

send_finger_tables(NetworkState,M) ->
    FingerTables = collectfingertables(NetworkState, dict:to_list(NetworkState), dict:new(),M),
    io:format("~n~p~n", [FingerTables]),
    send_finger_tables_nodes(dict:fetch_keys(FingerTables), NetworkState, FingerTables)
.

create_network(NumNodes, NumRequest) ->
    M = get_m(NumNodes),
    [ChordNodes, NetworkState] = create_nodes([], round(math:pow(2, M)), M, NumNodes, dict:new()),
    
    send_finger_tables(NetworkState,M),
    
    stabilize(ChordNodes, NetworkState),
    send_messages_and_kill(ChordNodes, NumNodes, NumRequest, M, NetworkState)
.



main(NumNodes, NumRequest) ->
    register(mainprocess, spawn(chord, create_network, [NumNodes, NumRequest]))
.
