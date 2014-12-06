-module(nms_rabbitmq_task).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([do_consume/3]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

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
    io:format("Args must be list!Error!"),
    {error, args_not_list}.

stop(TaskPid) ->
    gen_server:call(TaskPid, stop, infinity).

%% TaskPid -> nms_rabbitmq_task 进程 pid
%% TaskConPid -> nms_task_control 进程 pid
do_consume(TaskPid, TaskConPid, QueueN) ->
    gen_server:call(TaskPid, {do_consume, TaskConPid, QueueN}, infinity).


%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

setup_queue(Channel, QueueN) ->
    #'queue.declare_ok'{} = 
        amqp_channel:call(Channel, #'queue.declare'{queue = nms_api:to_binary(QueueN)}).

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
            io:format("[nms_rabbitmq_task] RabbitqCon Pid = ~p~n", [Connection]),
            case amqp_connection:open_channel(
                Connection, {amqp_direct_consumer, [self()]}) of
                {ok, Channel} ->
                    io:format("[nms_rabbitmq_task] Channel = ~p~n", [Channel]),
                    {ok, #state{rabbit_con=Connection, rabbit_chan=Channel}};
                {error, Error} ->
                    io:format("[nms_rabbitmq_task] channel_error Reason = ~p~n", [Error]),
                    {stop, {channel_error, Error}}
            end;        
        {error, Error} ->
            io:format("[nms_rabbitmq_task] connection_error Reason = ~p~n", [Error]),
            {stop, {connection_error, Error}}
    end.

handle_call({do_consume, TaskCon, QueueN}, _From, #state{rabbit_chan=Channel}=State) ->
    setup_queue(Channel, QueueN),
    setup_consumer(Channel, QueueN),
    io:format("[nms_rabbitmq_task] Consumer Setup!~n"),
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
    io:format("[nms_rabbitmq_task] Recv #'basic.cancel'{}, need do something ~n", []),
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
