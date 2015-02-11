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

-module(nms_rabbitmq_task).
-include_lib("amqp_client/include/amqp_client.hrl").
-behaviour(gen_server).

-export([init/1, 
         terminate/2, 
         code_change/3, 
         handle_call/3, 
         handle_cast/2,
         handle_info/2]).

-export([start_link/1, stop/1]).
-export([do_consume/5]).

-record(state, {
        rabbit_con   = undefined,
        rabbit_chan  = undefined,
        task_control = undefined
    }).


%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start_link(Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [Args], []);
start_link(_) ->
    lager:error("[RabbitmqTask] Args must be list! Error!"),
    {error, args_not_list}.

stop(TaskPid) ->
    gen_server:call(TaskPid, stop, infinity).

%% TaskPid -> nms_rabbitmq_task 进程 pid
%% TaskConPid -> nms_task_control 进程 pid
do_consume(TaskPid, TaskConPid, QueueN, ExchangeN, RoutingKey) ->
    gen_server:call(TaskPid, {do_consume, TaskConPid, QueueN, ExchangeN, RoutingKey}, infinity).


%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

setup_queue(Channel, QueueN) ->
    #'queue.declare_ok'{} = 
        amqp_channel:call(Channel, #'queue.declare'{queue = nms_api:to_binary(QueueN), durable = true}).

setup_bind(Channel, QueueN, ExchangeN, RoutingKey) ->
    #'queue.bind_ok'{} = 
        amqp_channel:call(Channel, #'queue.bind'{
            queue=nms_api:to_binary(QueueN), 
            exchange=nms_api:to_binary(ExchangeN), 
            routing_key=nms_api:to_binary(RoutingKey)}).

setup_consumer(Channel, QueueN) ->
    #'basic.consume_ok'{} =        
        amqp_channel:call(Channel, #'basic.consume'{queue = QueueN, no_ack = true}).


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([Args]) ->
    Params = #amqp_params_network{
                    username     = proplists:get_value(username, Args, <<"guest">>),
                    password     = proplists:get_value(password, Args, <<"guest">>),
                    virtual_host = proplists:get_value(virtual_host, Args, <<"/">>),
                    channel_max  = proplists:get_value(channel_max, Args, 0),
                    ssl_options  = proplists:get_value(ssl_options, Args, none),
                    host         = proplists:get_value(host, Args, "localhost")
                },
    case amqp_connection:start(Params) of 
        {ok, Connection} ->
            lager:info("[RabbitmqTask] Connection Pid = ~p~n", [Connection]),
            case amqp_connection:open_channel(Connection, {amqp_direct_consumer, [self()]}) of
                {ok, Channel} ->
                    lager:info("[RabbitmqTask] Channel Pid = ~p~n", [Channel]),
                    {ok, #state{rabbit_con=Connection, rabbit_chan=Channel}};
                {error, ChanErr} ->
                    lager:info("[RabbitmqTask] Channel Setup Failed! Error '~p'~n", [ChanErr]),
                    {stop, {channel_error, ChanErr}}
            end;        
        {error, ConnErr} ->
            lager:info("[RabbitmqTask] Connection Setup Failed! Error '~p'~n", [ConnErr]),
            {stop, {connection_error, ConnErr}}
    end.

handle_call({do_consume, TaskCon, QueueN, ExchangeN, RoutingKey}, 
        _From, #state{rabbit_chan=Channel}=State) ->
    setup_queue(Channel, QueueN),
    setup_bind(Channel, QueueN, ExchangeN, RoutingKey),
    setup_consumer(Channel, QueueN),

    lager:notice("[RabbitmqTask] Consumer Setup Success!"),
    {reply, ok, State#state{task_control=TaskCon}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({#'basic.consume'{}, _Pid}, State) ->
    {noreply, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel'{}, State) ->
    lager:info("[RabbitmqTask] Recv #'basic.cancel'{}, need do something ~n", []),
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{}, #amqp_msg{payload=Payload}},
        #state{task_control=TaskCon}=State) ->
    TaskCon ! {#'basic.deliver'{}, Payload},
    {noreply, State}.

terminate(_Reason, #state{rabbit_con=Connection,rabbit_chan=Channel}=_State) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok;

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
