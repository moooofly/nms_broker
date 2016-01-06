%% Copyright (c) 2014-2016, Moooofly <http://my.oschina.net/moooofly/blog>
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

-module(nms_task_control).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/4]).

-export([do_consume/4]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {
			tref       = undefined :: undefined | nms_api:ref(), 
			mq_task    = undefined,
			redis_task = undefined,
			mysql_task = undefined
	}).


-define(CPU_THRESHOLD_DEFAULT,  <<"80">>).
-define(MEM_THRESHOLD_DEFAULT,  <<"80">>).
-define(DISK_THRESHOLD_DEFAULT, <<"80">>).
-define(NET_THRESHOLD_DEFAULT,  <<"62914560">>).  %%  62914560 = 60 * 1024 * 1024

-define(PAS_THRESHOLD_DEFAULT, 5000).   %% 接入数量
-define(MTS_THRESHOLD_DEFAULT, 400).    %% 呼叫对数量
-define(UPU_THRESHOLD_DEFAULT, 10000).  %% 在线数量
-define(NMS_THRESHOLD_DEFAULT, 10000).  %% 接入数量

%% 告警设备类型定义，同 warning_handler.erl 中定义
-define(P_SERVER,   0).
-define(L_SERVER,   1).
-define(TERMINAL,   2).

%% 告警产生和告警修复
-define(WARN_ON,    "0").   %% 告警产生
-define(WARN_OFF,   "1").   %% 告警修复

%% 终端会议状态定义 MT_STATE
-define(IN_MULTI_CONF,    1).   %% 在多点会议中
-define(IN_P2P_CONF,      2).   %% 在点对点会议中
-define(NOT_IN_CONF,      3).   %% 不在会议中

-define(COLLECTOR_TIMEOUT, 30 * 1000). %% collector 心跳保活时间


%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start_link(TRef, MQTask, RedisTask, MySQLTask) ->
    gen_server:start_link(?MODULE, [TRef, MQTask, RedisTask, MySQLTask], []).


do_consume(TaskConPid, QueueN, ExchangeN, RoutingKey) ->
    gen_server:call(TaskConPid, {do_consume, QueueN, ExchangeN, RoutingKey}, infinity).

%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

cancel_timer(Ref) ->
    case erlang:cancel_timer(Ref) of
    false ->
        receive
            {timeout, Ref, _} -> 0
        after 0 -> false
        end;
    RemainingTime ->
        RemainingTime
    end.

%% 此函数内部均没有进行异常处理，若 rfc4627:get_field 返回 undefined 则崩溃
msg_parser(JsonObj, #state{redis_task=RedisTask, mysql_task=MySQLTask}) ->
    ok.


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([TRef, MQTask, RedisTask, MySQLTask]) ->
    process_flag(trap_exit, true),

    ok = nms_config:set_task_control(TRef, self()),    
    lager:info("[TaskControl] init => set (TaskRef, TaskConPid) = (~p, ~p)~n", [TRef, self()]),

    MQTaskPid = case MQTask of
        none -> none;
        _ -> nms_config:get_rabbitmq_task(TRef)
    end,

    RedisTaskPid = case RedisTask of
        none -> none;
        _ -> nms_config:get_redis_task(TRef)
    end,

    MySQLTaskPid = case MySQLTask of
        none -> none;
        _ -> nms_config:get_mysql_task(TRef)
    end,
    
    lager:info("[TaskControl] init => [old] MQTask(~p) RedisTask(~p) MySQLTask(~p)~n", 
        [MQTask, RedisTask, MySQLTask]),
    lager:info("[TaskControl] init => [new] MQTask(~p) RedisTask(~p) MySQLTask(~p)~n", 
        [MQTaskPid, RedisTaskPid, MySQLTaskPid]),

    {ok, #state{tref=TRef, mq_task=MQTaskPid, redis_task=RedisTaskPid, mysql_task=MySQLTaskPid}}.

handle_call( {do_consume, QueueN, ExchangeN, RoutingKey}, 
        _From, #state{mq_task=MQTask} = State) ->
    nms_rabbitmq_task:do_consume(MQTask, self(), QueueN, ExchangeN, RoutingKey),
    lager:info("[TaskControl] handle_call => recv {do_consume, ~p, ~p, ~p}~n", 
        [QueueN, ExchangeN, RoutingKey]),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info( {#'basic.deliver'{}, Payload}, State) ->
    %%io:format("+++++  Recv basic.deliver  +++++~n~n"),
    %%io:format("Payload=~p~n~n", [Payload]),
    %%io:format("+++++  Recv basic.deliver  +++++~n~n"),
    case rfc4627:decode(Payload) of
        {ok, Result, Remainder} ->
            lager:notice("#####  rfc4627:decode  #####"),
            io:format("Result=~p~n~nRemainder=~p~n~n", [Result, Remainder]),
            lager:info("~n~nResult=~p~n~nRemainder=~p~n~n", [Result, Remainder]),
            lager:notice("#####  rfc4627:decode  #####"),
            msg_parser(Result, State);

        {error, Error} ->
            lager:error("after rfc4627:decode   --> ~nerror=[~p]~n", [Error])
    end,
    {noreply, State};

handle_info({'EXIT', Pid, {restart, From}}, State) ->
    lager:warning("[TaskControl] recv {'EXIT', ~p, {restart, ~p}}~n", [Pid, From]),
    {stop, need_to_restart, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    lager:warning("[TaskControl] terminate => Reason(~p)~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
