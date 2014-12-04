-module(nms_manager_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(MRef) ->
    {ok, MSup} = supervisor:start_link(?MODULE, []),
    io:format("[1] MSup=~p~n", [MSup]),
    {ok, MTSup}    = supervisor:start_child(
                         MSup, {nms_manager_task_sup,
                               {nms_manager_task_sup, start_link, []},
                               transient, infinity, supervisor,
                               [nms_manager_task_sup]}),
    io:format("[2] MTSup=~p~n", [MTSup]),

    {ok, MControl} = supervisor:start_child(
                         MSup, {nms_manager_control,
                               {nms_manager_control, start_link, [MTSup, MRef]},
                               transient, brutal_kill, worker,
                               [nms_manager_control]}),
    io:format("[3] MControl=~p~n", [MControl]),
    {ok, MSup, MControl}.

%%---------------------------------------------------------------------------
%% supervisor callback
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
