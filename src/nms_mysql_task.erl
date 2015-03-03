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

-module(nms_mysql_task).
-behaviour(gen_server).

-export([init/1,
         terminate/2,
         code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-export([start_link/2]).

-record(pool, {
            pool_id = undefined :: undefined | nms_api:ref(), 
            options = [] :: list()
        }).

-record(state, {
            pool_info = #pool{},
            status    = off :: off | on
    }).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

-spec start_link(PoolId, Args) -> {ok, Pid} | {error, Reason} when
    PoolId :: nms_api:ref(),
    Args   :: list(),
    Pid    :: pid(),
    Reason :: term().
start_link(PoolId, Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [PoolId, Args], []);
start_link(_, _) ->
    lager:error("[MySQLTask] Args must be list! Error!"),
    {error, args_not_list}.

%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

-spec ensure_emysql_started() -> ok.
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
                    lager:notice("[MySQLTask] PoolId(~p) create Success!~n", [PoolId]),
                    ok;        
                {error, pool_already_exists} ->
                    lager:notice("[MySQLTask] PoolId(~p) already Exists~n", [PoolId]),
                    ok;
                {error, Error} ->
                    lager:error("[MySQLTask] get {error, ~p}~n", [Error]),
                    not_ok
            end;
        on ->
            ok
    end.


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([PoolId, Args]) ->
%%    Size      = proplists:get_value(size, Args, 1),
%%    User      = proplists:get_value(user, Args, "root"),
%%    Password  = proplists:get_value(password, Args, "root"),
%%    Host      = proplists:get_value(host, Args, "172.16.81.101"),
%%    Port      = proplists:get_value(port, Args, 3306),
%%    Database  = proplists:get_value(database, Args, "nms_db"),
%%    Encoding  = proplists:get_value(encoding, Args, utf8),
%%    StartCmds = proplists:get_value(start_cmds, Args, []),
%%    ConnectTimeout = proplists:get_value(connect_timeout, Args, infinity),

    Pool = #pool{pool_id=PoolId, options=Args},

%%    lager:notice("~s~n", [string:chars($-,72)]),
%%    lager:notice("~n"
%%                "    PoolId=~p~n"
%%                "    Size=~p~n"
%%                "    User=~p~n"
%%                "    Password=~p~n"
%%                "    Host=~p~n"
%%                "    Port=~p~n"
%%                "    Database=~p~n"
%%                "    Encoding=~p~n"
%%                "    StartCmds=~p~n"
%%                "    ConnectTimeout=~p~n", 
%%        [PoolId,Size,User,Password,Host,Port,Database,Encoding,StartCmds,ConnectTimeout]), 
%%    lager:notice("~s~n", [string:chars($-,72)]),

    case catch ensure_emysql_started() of
        ok ->
            {ok, #state{pool_info=Pool, status=off}};
        {error, Error} ->
            {stop, {emysql_startapp_error, Error}}
    end.    


handle_call( {get_terminal_mem_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_terminal_mem_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_terminal_mem_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            lager:warning("[MySQLTask] get_terminal_mem_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_terminal_cpu_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_terminal_cpu_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_terminal_cpu_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            lager:warning("[MySQLTask] get_terminal_cpu_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_terminal_version_statistic, DomainMoid, DevMoid, E164, Type, Oem, Version, Recommend}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_terminal_version_statistic'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_terminal_version_statistic(Pool#pool.pool_id, DomainMoid, 
                    DevMoid, E164, Type, Oem, Version, Recommend) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] add_terminal_version_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_warning_repair_statistic, DomainMoid, DevMoid, WarningCode, WarningStatus, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_warning_repair_statistic'~n", [Pool#pool.pool_id]),

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
            lager:warning("[MySQLTask] add_warning_repair_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {del_unrepaired_warning, DevGuid, DomainGuid, WarningCode}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'del_unrepaired_warning'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:del_unrepaired_warning(Pool#pool.pool_id, DevGuid, DomainGuid, WarningCode) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] del_unrepaired_warning failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_repaired_warning, DevMoid, DevType, DomainMoid, WarningCode, Level, Description, StartTime, ResolveTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_repaired_warning'~n", [Pool#pool.pool_id]),

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
            lager:warning("[MySQLTask] add_repaired_warning failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_unrepaired_warning, DevMoid, DevType, DomainMoid, WarningCode, Level, Description, StartTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_unrepaired_warning'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:add_unrepaired_warning(Pool#pool.pool_id, DevMoid, DevType, 
                        DomainMoid, WarningCode, Level, Description, StartTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] add_unrepaired_warning failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_warning_code_detail, WarningCode}, _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_warning_code_detail'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:get_warning_code_detail(Pool#pool.pool_id, WarningCode) of
                [] ->
                    lager:warning("[MySQLTask] ###### have got no warning code information ######~n", []),
                    {reply, [], State#state{status=on}};
                [Value] ->
                    {reply, Value, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] get_warning_code_detail failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_device_warning_by_code, DomainMoid, DevMoid, WarningCode}, 
                _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_device_warning_by_code'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case warning_handler:get_device_warning_by_code(Pool#pool.pool_id, DomainMoid, DevMoid, WarningCode) of
                [] ->
                    {reply, {non_exist, []}, State#state{status=on}};
                [Value] ->
                    {reply, {exist, Value}, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] get_device_warning_by_code failed!~n", []),
            {reply, pool_init_failed, State}
    end;


handle_call( {add_netcard_statistic, DomainMoid, DevMoid, CardID, PortIn, PortOut, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_netcard_statistic'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_netcard_statistic(Pool#pool.pool_id, DomainMoid, 
                    DevMoid, CardID, PortIn, PortOut, StatisticTime) of
                {ok,success} ->                    
                    {reply, {ok,success}, State#state{status=on}};
                {error, Error} ->
                     {reply, {error, Error}, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] add_netcard_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_disk_statistic, DomainMoid, DevMoid, DiskPct, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_disk_statistic'~n", [Pool#pool.pool_id]),

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
            lager:warning("[MySQLTask] add_disk_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_memory_statistic, DomainMoid, DevMoid, MemoryPct, StatisticTime}, 
        _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_memory_statistic'~n", [Pool#pool.pool_id]),

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
            lager:warning("[MySQLTask] add_memory_statistic failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {add_cpu_statistic, DomainMoid, DevMoid, CpuID, Cpu, StatisticTime}, 
                _From, #state{pool_info=Pool,status=Status}=State) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'add_cpu_statistic'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            case statistic_handler:add_cpu_statistic(Pool#pool.pool_id,DomainMoid,DevMoid,CpuID,Cpu,StatisticTime) of
                {ok,success} ->
                    {reply, ok, State#state{status=on}};
                {error,_} ->
                    lager:warning("TODO: need code something!~n"),
                    {reply, not_ok, State#state{status=on}}
            end;            
        _ ->
            lager:warning("[MySQLTask] add_cpu_statistic failed!~n"),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_net_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_server_net_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_net_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            lager:warning("[MySQLTask] get_server_net_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_disk_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_server_disk_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_disk_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            lager:warning("[MySQLTask] get_server_disk_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_mem_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_server_mem_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_mem_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            lager:warning("[MySQLTask] get_server_mem_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call( {get_server_cpu_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_server_cpu_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            Value = system_set_handler:get_server_cpu_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, Value}, State#state{status=on}};            
        _ ->
            lager:warning("[MySQLTask] get_server_cpu_limit failed!~n", []),
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