-module(nms_task_sup).

-behaviour(supervisor).

-export([start_link/4]).
-export([init/1]).

start_link(TRef, MQArgs, RedisArgs, MySQLArgs) ->
    {ok, TSup} = supervisor:start_link(?MODULE, []),
    io:format("[4] TSup=~p~n", [TSup]),

    if 
        MQArgs =/= none ->
            {ok, MQTask}    = supervisor:start_child(
                                TSup, {nms_rabbitmq_task,
                                      {nms_rabbitmq_task, start_link, [MQArgs]},
                                      transient, brutal_kill, worker,
                                      [nms_rabbitmq_task]}),
            io:format("[5] MQTask=~p~n", [MQTask]);
        true -> 
            MQTask = none,
            io:format("[5] MQArgs is none, MQTask need not start~n")
    end,

    if 
        RedisArgs =/= none ->
            {ok, RedisTask} = supervisor:start_child(
                                TSup, {nms_redis_task, 
                                      {nms_redis_task, start_link, [RedisArgs]},
                                      transient, brutal_kill, worker,
                                      [nms_redis_task]}),
            io:format("[6] RedisTask=~p~n", [RedisTask]);
        true ->
            RedisTask = none,
            io:format("[6] RedisArgs is none, RedisTask need not start~n")
    end,

    if 
        MySQLArgs =/= none ->
            {ok, MySQLTask} = supervisor:start_child(
                                TSup, {nms_mysql_task,
                                {nms_mysql_task, start_link, [TRef, MySQLArgs]},
                                transient, brutal_kill, worker,
                                [nms_mysql_task]}),
            io:format("[7] MySQLTask=~p~n", [MySQLTask]);
        true ->
            MySQLTask = none,
            io:format("[7] MySQLArgs is none, MySQLTask need not start~n")
    end,

    {ok, TaskControl} = supervisor:start_child(
                         TSup, {nms_task_control, 
                               {nms_task_control, start_link, 
                                   [TRef, MQTask, RedisTask, MySQLTask]},
                               transient, brutal_kill, worker,
                               [nms_task_control]}),
    io:format("[8] TaskControl=~p~n", [TaskControl]),
    {ok, TSup, TaskControl}.

%%---------------------------------------------------------------------------
%% supervisor callback
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
