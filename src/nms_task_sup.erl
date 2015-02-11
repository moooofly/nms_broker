%% Copyright (c) 2014-2015, Moooofly <http://my.oschina.net/moooofly/blog>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

-module(nms_task_sup).

-behaviour(supervisor).

-export([start_link/4]).
-export([init/1]).

start_link(TRef, MQArgs, RedisArgs, MySQLArgs) ->
    {ok, TSup} = supervisor:start_link(?MODULE, []),
    lager:info("[TaskSup] TaskSup Pid = ~p~n", [TSup]),

    if 
        MQArgs =/= none ->
            {ok, MQTask}    = supervisor:start_child(
                                TSup, {nms_rabbitmq_task,
                                      {nms_rabbitmq_task, start_link, [MQArgs]},
                                      transient, brutal_kill, worker,
                                      [nms_rabbitmq_task]}),
            lager:info("[TaskSup] RabbitmqTask Pid = ~p~n", [MQTask]);
        true -> 
            MQTask = none,
            lager:notice("[TaskSup] MQArgs is unavailable, RabbitmqTask need not to start!")
    end,

    if 
        RedisArgs =/= none ->
            {ok, RedisTask} = supervisor:start_child(
                                TSup, {nms_redis_task, 
                                      {nms_redis_task, start_link, [RedisArgs]},
                                      transient, brutal_kill, worker,
                                      [nms_redis_task]}),
            lager:info("[TaskSup] RedisTask Pid = ~p~n", [RedisTask]);
        true ->
            RedisTask = none,
            lager:notice("[TaskSup] RedisArgs is unavailable, RedisTask need not to start!")
    end,

    if 
        MySQLArgs =/= none ->
            {ok, MySQLTask} = supervisor:start_child(
                                TSup, {nms_mysql_task,
                                {nms_mysql_task, start_link, [TRef, MySQLArgs]},
                                transient, brutal_kill, worker,
                                [nms_mysql_task]}),
            lager:info("[TaskSup] MySQLTask Pid = ~p~n", [MySQLTask]);
        true ->
            MySQLTask = none,
            lager:notice("[TaskSup] MySQLArgs is unavailable, MySQLTask need not to start!")
    end,

    {ok, TaskControl} = supervisor:start_child(
                         TSup, {nms_task_control, 
                               {nms_task_control, start_link, 
                                   [TRef, MQTask, RedisTask, MySQLTask]},
                               transient, brutal_kill, worker,
                               [nms_task_control]}),
    lager:info("[TaskSup] TaskControl Pid = ~p~n", [TaskControl]),
    {ok, TSup, TaskControl}.

%%---------------------------------------------------------------------------
%% supervisor callback
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
