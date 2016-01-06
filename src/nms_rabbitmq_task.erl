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

-module(nms_rabbitmq_task).
-include_lib("amqp_client/include/amqp_client.hrl").
-behaviour(gen_server).

-export([init/1, 
         terminate/2, 
         code_change/3, 
         handle_call/3, 
         handle_cast/2,
         handle_info/2]).

-export([start_link/2, stop/1]).
-export([do_consume/5]).

-record(consumer_params, {
            queue       = undefined,
            exchange    = <<"">>,
            routingkey  = <<"">>
    }).

-record(state, {
            tref            = undefined :: undefined | nms_api:ref(),
            conn_params     = undefined,
            consumer_params = #consumer_params{},
            rabbit_con      = undefined,
            rabbit_chan     = undefined
    }).


%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start_link(TRef, Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [TRef, Args], []);
start_link(_, _) ->
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

init([TRef, Args]) ->
    process_flag(trap_exit, true),

    ConnParams = #amqp_params_network{
                    username     = proplists:get_value(username, Args, <<"guest">>),
                    password     = proplists:get_value(password, Args, <<"guest">>),
                    virtual_host = proplists:get_value(virtual_host, Args, <<"/">>),
                    channel_max  = proplists:get_value(channel_max, Args, 0),
                    ssl_options  = proplists:get_value(ssl_options, Args, none),
                    host         = proplists:get_value(host, Args, "localhost")
                },
    ConsumerParams = #consumer_params{
                    queue        = proplists:get_value(queue, Args, undefined),
                    exchange     = proplists:get_value(exchange, Args, <<"">>),
                    routingkey   = proplists:get_value(routing_key, Args, <<"">>)
                },

    State = #state{tref=TRef, conn_params=ConnParams, consumer_params=ConsumerParams},

    %% 异步实现方式（需要继续研究）
    % self() ! reconnect,
    % {ok, State}.

    case amqp_connection:start(ConnParams) of 
        {ok, Connection} ->
            lager:info("[RabbitmqTask] init => create Connection Success! Pid = ~p~n", [Connection]),
            erlang:monitor(process, Connection),
            case amqp_connection:open_channel(Connection, {amqp_direct_consumer, [self()]}) of
                {ok, Channel} ->
                    lager:info("[RabbitmqTask] init => create Channel Success! Pid = ~p~n", [Channel]),
                    erlang:monitor(process, Channel),
                    self() ! start_consume,
                    nms_config:set_rabbitmq_task(TRef, self()),
                    {ok, State#state{rabbit_con=Connection, rabbit_chan=Channel}};
                {error, ChanErr} ->
                    lager:error("[RabbitmqTask] create Channel Failed! Error '~p'~n", [ChanErr]),
                    {stop, {create_channel_error, ChanErr}}
            end;        
        {error, ConnErr} ->
            lager:error("[RabbitmqTask] create Connection Failed! Error '~p'~n", [ConnErr]),
            {stop, {create_connection_error, ConnErr}}
    end.

handle_call({do_consume, _TaskCon, QueueN, ExchangeN, RoutingKey}, 
        _From, #state{rabbit_chan=Channel}=State) ->

    lager:info("[RabbitmqTask] Create Consumer => queue(~p) exchange(~p) routingkey(~p)~n", 
        [QueueN, ExchangeN, RoutingKey]),

    setup_queue(Channel, QueueN),
    setup_bind(Channel, QueueN, ExchangeN, RoutingKey),
    setup_consumer(Channel, QueueN),

    lager:info("[RabbitmqTask] Consumer Setup Success!"),
    {reply, ok, State};

handle_call({do_consume, _TaskCon, #consumer_params{queue=QueueN,exchange=ExchangeN,routingkey=RoutingKey}=_}, 
        _From, #state{rabbit_chan=Channel}=State) ->

    lager:info("[RabbitmqTask] Create Consumer => queue(~p) exchange(~p) routingkey(~p)~n", 
        [QueueN, ExchangeN, RoutingKey]),

    setup_queue(Channel, QueueN),
    setup_bind(Channel, QueueN, ExchangeN, RoutingKey),
    setup_consumer(Channel, QueueN),

    lager:info("[RabbitmqTask] Consumer Setup Success!"),
    {reply, ok, State};

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

handle_info({#'basic.deliver'{}, #amqp_msg{payload=Payload}}, #state{tref=TRef}=State) ->
    TaskConPid = nms_config:get_task_control(TRef),
    lager:info("[RabbitmqTask] Current TaskConPid(~p)~n", [TaskConPid]),
    TaskConPid ! {#'basic.deliver'{}, Payload},
    {noreply, State};

handle_info(start_consume, #state{rabbit_chan=Channel, consumer_params=Params}=State) ->

    lager:info("[RabbitmqTask] Create Consumer => queue(~p) exchange(~p) routingkey(~p) Channel(~p)~n", 
        [Params#consumer_params.queue, Params#consumer_params.exchange, Params#consumer_params.routingkey, Channel]),

    setup_queue(Channel, list_to_binary(Params#consumer_params.queue) ),
    setup_bind(Channel, 
        list_to_binary(Params#consumer_params.queue), 
        list_to_binary(Params#consumer_params.exchange), 
        list_to_binary(Params#consumer_params.routingkey)),
    setup_consumer(Channel, list_to_binary(Params#consumer_params.queue)),

    lager:info("[RabbitmqTask] Create Consumer Success!"),
    {noreply, State};


handle_info(reconnect, #state{tref=TRef, conn_params=ConnParams}=State) ->

    case amqp_connection:start(ConnParams) of 
        {ok, NewConn} ->
            lager:info("[RabbitmqTask] Create Connection Success! Pid = ~p~n", [NewConn]),
            erlang:monitor(process, NewConn),
            case amqp_connection:open_channel(NewConn, {amqp_direct_consumer, [self()]}) of
                {ok, NewChan} ->
                    lager:info("[RabbitmqTask] Create Channel Success! Pid = ~p~n", [NewChan]),
                    erlang:monitor(process, NewChan),
                    self() ! start_consume,
                    nms_config:set_rabbitmq_task(TRef, self()),
                    {noreply, State#state{rabbit_con=NewConn, rabbit_chan=NewChan}};
                {error, ChanErr} ->
                    lager:error("[RabbitmqTask] Create Channel Failed! Error '~p'~n", [ChanErr]),
                    {stop, {re_create_channel_error, ChanErr}}
            end;        
        {error, ConnErr} ->
            lager:warning("[RabbitmqTask] re-Create Connection Failed! Error '~p'~n", [ConnErr]),
            erlang:send_after(5000, self(), reconnect),
            {noreply, State}
    end;

handle_info({'DOWN', MRef, process, Pid, Info}, 
    #state{tref=TRef, rabbit_con=OldConn,rabbit_chan=OldChan,conn_params=ConnParams}=State) ->

    case Pid of 
        OldConn ->
            lager:info("[RabbitmqTask] recv {'DOWN', ~p, process, ~p, ~p}, Connection down!~n", [MRef, Pid, Info]),

            case amqp_connection:start(ConnParams) of 
                {ok, NewConn} ->
                    lager:info("[RabbitmqTask] re-Create Connection Success! Pid = ~p~n", [NewConn]),
                    erlang:monitor(process, NewConn),
                    case amqp_connection:open_channel(NewConn, {amqp_direct_consumer, [self()]}) of
                        {ok, NewChan} ->
                            lager:info("[RabbitmqTask] re-Create Channel Success! Pid = ~p~n", [NewChan]),
                            erlang:monitor(process, NewChan),
                            self() ! start_consume,
                            nms_config:set_rabbitmq_task(TRef, self()),
                            {noreply, State#state{rabbit_con=NewConn, rabbit_chan=NewChan}};
                        {error, ChanErr} ->
                            lager:error("[RabbitmqTask] re-Create Channel Failed! Error '~p'~n", [ChanErr]),
                            {stop, {re_create_channel_error, ChanErr}}
                    end;        
                {error, ConnErr} ->
                    lager:warning("[RabbitmqTask] re-Create Connection Failed! Error '~p'~n", [ConnErr]),
                    erlang:send_after(5000, self(), reconnect),
                    {noreply, State}
            end;

        OldChan    ->
            lager:info("[RabbitmqTask] recv {'DOWN', ~p, process, ~p, ~p}, Channel down!~n", [MRef, Pid, Info]),
            {noreply, State};

        _          ->
            lager:info("[RabbitmqTask] recv {'DOWN', ~p, process, ~p, ~p}, something down!~n", [MRef, Pid, Info]),
            {noreply, State}
    end;

%% Info -> {'EXIT',<xx.xx.xx>,some}
handle_info(Info, State) ->
    lager:warning("[RabbitmqTask] handle_info => Info(~p)~n", [Info]),
    {stop, abnormal, State}.

terminate(Reason, #state{rabbit_con=Connection,rabbit_chan=Channel}=_State) ->
    lager:warning("[RabbitmqTask] terminate Reason ### ~p ###~n", [Reason]),
    lager:warning("[RabbitmqTask] close AMQP channel(~p) and connection(~p)~n", [Channel, Connection]),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
