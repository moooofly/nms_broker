-module(nms_mysql_task).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).


-record(pool, {
            pool_id = undefined, 
            options = []
        }).

-record(state, {
            pool_info   = #pool{},
            status      = off      %% off | on
    }).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start_link(PoolId, Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [PoolId, Args], []);
start_link(_, _) ->
    io:format("Args must be list!Error!"),
    {error, args_not_list}.


%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

ensure_emysql_started() ->
    case application_controller:get_master(emysql) of
        undefined ->
            crypto:start(),
            case emysql:start() of
                ok                                      -> ok;
                {error, {already_started, emysql}}      -> ok;
                {error, _} = E                          -> throw(E)
            end;
        _ ->
            ok
    end.

initialize_pool_status(Pool, Status) ->
    PoolId = Pool#pool.pool_id,
    case Status of
        off ->
            case emysql:add_pool(Pool#pool.pool_id, Pool#pool.options) of 
                ok ->
                    io:format("[nms_mysql_task] PoolId(~p) create Success!~n", [PoolId]),
                    ok;        
                {error, pool_already_exists} ->
                    io:format("[nms_mysql_task] PoolId(~p) already Exists~n", [PoolId]),
                    ok;
                {error, Error} ->
                    io:format("[nms_mysql_task] get {error, ~p}~n", [Error]),
                    not_ok
            end;
        on ->
            ok
    end.


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([PoolId, Args]) ->
    Size = proplists:get_value(size, Args, 1),
    User = proplists:get_value(user, Args, "root"),
    Password = proplists:get_value(password, Args, "root"),
    Host = proplists:get_value(host, Args, "172.16.81.101"),
    Port = proplists:get_value(port, Args, 3306),
    Database = proplists:get_value(database, Args, "nms_db"),
    Encoding = proplists:get_value(encoding, Args, utf8),
    StartCmds = proplists:get_value(start_cmds, Args, []),
    ConnectTimeout = proplists:get_value(connect_timeout, Args, infinity),

    Pool = #pool{pool_id=PoolId, options=Args},

    io:format("~n~s~n", [string:chars($-,72)]),

    io:format(  "    PoolId=~p~n"
                "    Size=~p~n"
                "    User=~p~n"
                "    Password=~p~n"
                "    Host=~p~n"
                "    Port=~p~n"
                "    Database=~p~n"
                "    Encoding=~p~n"
                "    StartCmds=~p~n"
                "    ConnectTimeout=~p~n", 
        [PoolId,Size,User,Password,Host,Port,Database,Encoding,StartCmds,ConnectTimeout]), 

    io:format("~n~s~n", [string:chars($-,72)]),

    case catch ensure_emysql_started() of
        ok ->
            io:format("[nms_mysql_task] init ok!~n", []),
            {ok, #state{pool_info=Pool, status=off}};
        {error, Error} ->
            {stop, {emysql_startapp_error, Error}}
    end.    


handle_call( {add_warning_repair_statistic, DomainMoid, DevMoid, WarningCode, WarningStatus, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'add_warning_repair_statistic'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_warning_repair_statistic(Pool#pool.pool_id, DomainMoid, 
                    DevMoid, WarningCode, WarningStatus, StatisticTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_warning_repair_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {del_unrepaired_warning, DevGuid, DomainGuid, WarningCode}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'del_unrepaired_warning'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:del_unrepaired_warning(Pool#pool.pool_id, DevGuid, DomainGuid, WarningCode) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] del_unrepaired_warning failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_repaired_warning, DevMoid, DevType, DomainMoid, WarningCode, Level, Description, StartTime, ResolveTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'add_repaired_warning'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:add_repaired_warning(Pool#pool.pool_id, DevMoid, DevType, 
                        DomainMoid, WarningCode, Level, Description, StartTime, ResolveTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_repaired_warning failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_unrepaired_warning, DevMoid, DevType, DomainMoid, WarningCode, Level, Description, StartTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'add_unrepaired_warning'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:add_unrepaired_warning(Pool#pool.pool_id, DevMoid, DevType, 
                        DomainMoid, WarningCode, Level, Description, StartTime) of
                [] ->
                    io:format("[nms_mysql_task] ###### have got no warning code information ######~n", []),
                    {reply, [], State#state{status=on}};
                Value ->
                    {reply, Value, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_unrepaired_warning failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_warning_code_detail, WarningCode}, _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'get_warning_code_detail'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:get_warning_code_detail(Pool#pool.pool_id, WarningCode) of
                [] ->
                    io:format("[nms_mysql_task] ###### have got no warning code information ######~n", []),
                    {reply, [], State#state{status=on}};
                Value ->
                    {reply, Value, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] get_warning_code_detail failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_device_warning_by_code, DomainMoid, DevMoid, WarningCode}, 
                _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'get_device_warning_by_code'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:get_device_warning_by_code(Pool#pool.pool_id, DomainMoid, DevMoid, WarningCode) of
                [] ->
                    {reply, {non_exist, []}, State#state{status=on}};
                [UnRepairedWarning] ->
                    {reply, {exist, UnRepairedWarning}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] get_device_warning_by_code failed!~n", []),
            {reply, pool_init_failed, State}
    end;


handle_call( {add_net_statistic, DomainMoid, DevMoid, CardID, PortIn, PortOut, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv add_net_statistic~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_net_statistic(Pool#pool.pool_id, DomainMoid, 
                    DevMoid, CardID, PortIn, PortOut, StatisticTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_net_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_disk_statistic, DomainMoid, DevMoid, DiskPct, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv add_disk_statistic~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_disk_statistic(Pool#pool.pool_id, DomainMoid, 
                    DevMoid, DiskPct, StatisticTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_disk_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_memory_statistic, DomainMoid, DevMoid, MemoryPct, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv add_memory_statistic~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_memory_statistic(Pool#pool.pool_id, DomainMoid, 
                    DevMoid, MemoryPct, StatisticTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_memory_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_cpu_statistic, DomainMoid, DevMoid, CpuID, Cpu, StatisticTime}, 
                _From, #state{pool_info=Pool,status=Status}=State) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'add_cpu_statistic'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_cpu_statistic(Pool#pool.pool_id,DomainMoid,DevMoid,CpuID,Cpu,StatisticTime) of
                {ok,success} ->
                    {reply, ok, State#state{status=on}};
                {error,_} ->
                    io:format("TODO: need code something!~n"),
                    {reply, not_ok, State#state{status=on}}
            end;            
        _ ->
            io:format("[nms_mysql_task] add_cpu_statistic failed!~n"),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_net_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv get_server_net_limit~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_net_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            io:format("[nms_mysql_task] get_server_net_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_disk_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv get_server_disk_limit~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_disk_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            io:format("[nms_mysql_task] get_server_disk_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_mem_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv get_server_mem_limit~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_mem_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            io:format("[nms_mysql_task] get_server_mem_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_cpu_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    io:format("[nms_mysql_task] PoolId(~p) <== Recv 'get_server_cpu_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_cpu_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            io:format("[nms_mysql_task] get_server_cpu_limit failed!~n", []),
            {reply, pool_init_failed, State}
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