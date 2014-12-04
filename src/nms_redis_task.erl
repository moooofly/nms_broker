-module(nms_redis_task).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {
        redis_con = undefined 
    }).


%% Args 为包含 host、port、database、password、reconnect_sleep 的元组列表
%% 如 [{host,"127.0.0.1"},{port,6379},{database,0},{password,""},{reconnect_sleep,100}]
start_link(Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [Args], []);
start_link(_) ->
    io:format("Args must be proplists!Error!"),
    {error, args_not_list}.

init([Args]) ->
    Host           = proplists:get_value(host, Args, "127.0.0.1"),
    Port           = proplists:get_value(port, Args, 6379),
    Database       = proplists:get_value(database, Args, 0),
    Password       = proplists:get_value(password, Args, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep, Args, 100),
    case eredis:start_link(Host, Port, Database, Password, ReconnectSleep) of    
        {ok, RedisCon} ->
            io:format("RedisCon Pid = ~p~n", [RedisCon]),
            {ok, #state{redis_con=RedisCon}};
        {error, Reason} ->
            io:format("[REDIS] connection_error Reason = ~p~n", [Reason]),
            {stop, {connection_error, Reason}}
    end.

handle_call( {del_physical_server_warning, DevMoid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_physical_server_warning(RedisCon,DevMoid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {add_physical_server_warning, DevMoid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:add_physical_server_warning(RedisCon,DevMoid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_net_resource, DevMoid, PortIn, PortOut}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_net_resource(RedisCon,DevMoid,PortIn,PortOut),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_mem_resource, DevMoid, Mem}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_mem_resource(RedisCon,DevMoid,Mem),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_disk_resource, DevMoid, Disk}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_disk_resource(RedisCon,DevMoid,Disk),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_cpu_resource, DevMoid, Cpu}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_cpu_resource(RedisCon,DevMoid,Cpu),
            {reply, Result, State}
    end;

handle_call( {get_physical_server_info, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            case physical_server_handler:get_physical_server_info(RedisCon,DevMoid) of
                {error,<<"Key Error">>} ->
                    { reply, {error,<<"Key Error">>}, State };
                {DevMoid,DevGuid,DomainMoid,Name,Location,IP} ->
                    {reply, {ok,{DevMoid,DevGuid,DomainMoid,Name,Location,IP}}, State}
            end
    end;

handle_call( {get_server_net_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
            Result = system_set_handler:get_server_net_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_server_disk_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
            Result = system_set_handler:get_server_disk_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_server_mem_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
            Result = system_set_handler:get_server_mem_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_server_cpu_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
            Result = system_set_handler:get_server_cpu_limit_redis(RedisCon),
            {reply, Result, State}
    end;
    
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.