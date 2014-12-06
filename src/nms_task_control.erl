-module(nms_task_control).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/4]).

-export([do_consume/2]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {
        tref       = undefined,
		mq_task    = undefined,
		redis_task = undefined,
		mysql_task = undefined
	}).


-define(CPU_THRESHOLD_DEFAULT, 80).
-define(MEM_THRESHOLD_DEFAULT, 80).
-define(DISK_THRESHOLD_DEFAULT, 80).
-define(NET_THRESHOLD_DEFAULT, 60 * 1024 * 1024).  %% 需要和孙涛确认上报信息的单位

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

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start_link(TRef, MQTask, RedisTask, MySQLTask) ->
    gen_server:start_link(?MODULE, [TRef, MQTask, RedisTask, MySQLTask], []).


do_consume(TaskConPid, QueueN) when is_binary(QueueN) andalso QueueN =/= <<>> ->
    gen_server:call(TaskConPid, {do_consume, QueueN}, infinity);
do_consume(TaskConPid, _) ->
    gen_server:call(TaskConPid, {do_consume, <<"hello">>}, infinity).


%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

%% 区分设备类型（物理设备|逻辑设备|终端设备）
devtype_distinguish(Devtype) ->
    String =    case Devtype of

                    %% 行业终端
                    <<"SERVICE_KDV_MT_KDV7620">>    -> {terminal, 'KDV7620'};
                    <<"SERVICE_KDV_MT_KDV7610">>    -> {terminal, 'KDV7610'};
                    <<"SERVICE_KDV_MT_KDV7210">>    -> {terminal, 'KDV7210'};
                    <<"SERVICE_KDV_MT_TS6610">>     -> {terminal, 'TS6610'};
                    <<"SERVICE_KDV_MT_TS5610">>     -> {terminal, 'TS5610'};
                    <<"SERVICE_KDV_MT_KDV8010A">>   -> {terminal, 'KDV8010A'};
                    <<"SERVICE_KDV_MT_8010">>       -> {terminal, '8010'};
                    <<"SERVICE_KDV_MT_8010A">>      -> {terminal, '8010A'};
                    <<"SERVICE_KDV_MT_8010C">>      -> {terminal, '8010C'};
                    <<"SERVICE_KDV_MT_8000H_M">>    -> {terminal, '8000H_M'};
                    <<"SERVICE_KDV_MT_RBS2KC">>     -> {terminal, 'RBS2KC'};
                    <<"SERVICE_KDV_MT_RBS2K">>      -> {terminal, 'RBS2K'};
                    <<"SERVICE_KDV_MT_RBS2KE">>     -> {terminal, 'RBS2KE'};
                    <<"SERVICE_KDV_MT_TS6000">>     -> {terminal, 'TS6000'};
                    <<"SERVICE_KDV_MT_TS5000">>     -> {terminal, 'TS5000'};
                    <<"SERVICE_KDV_MT_7820_A">>     -> {terminal, '7820_A'};
                    <<"SERVICE_KDV_MT_7820_B">>     -> {terminal, '7820_B'};
                    <<"SERVICE_KDV_MT_7920_A">>     -> {terminal, '7920_A'};
                    <<"SERVICE_KDV_MT_7920_B">>     -> {terminal, '7920_B'};
                    <<"SERVICE_KDV_MT_7921_H">>     -> {terminal, '7921_H'};
                    <<"SERVICE_KDV_MT_7921_L">>     -> {terminal, '7921_L'};
                    <<"SERVICE_KDV_MT_KDV7810">>    -> {terminal, 'KDV7810'};
                    <<"SERVICE_KDV_MT_KDV7910">>    -> {terminal, 'KDV7910'};
                    <<"SERVICE_KDV_MT_KDV7920">>    -> {terminal, 'KDV7920'};
                    <<"SERVICE_KDV_MT_KDV7820">>    -> {terminal, 'KDV7820'};
                    <<"SERVICE_KDV_MT_KDV7921">>    -> {terminal, 'KDV7921'};
                    <<"SERVICE_KDV_MT_KDV1000">>    -> {terminal, 'KDV1000'};
                    <<"SERVICE_KDV_MT_H600">>       -> {terminal, 'H600'};
                    <<"SERVICE_KDV_MT_H600-L">>     -> {terminal, 'H600-L'};
                    <<"SERVICE_KDV_MT_H700">>       -> {terminal, 'H700'};
                    <<"SERVICE_KDV_MT_H800">>       -> {terminal, 'H800'};
                    <<"SERVICE_KDV_MT_H900">>       -> {terminal, 'H900'};
                    <<"SERVICE_KDV_MT_PCMT">>       -> {terminal, 'PCMT'};

                    %% TS终端
                    <<"SERVICE_TS_MT_IPAD">>        -> {terminal, 'IPAD'};
                    <<"SERVICE_TS_MT_5610">>        -> {terminal, '5610'};
                    <<"SERVICE_TS_MT_H323MT">>      -> {terminal, 'H323MT'};
                    <<"SERVICE_TS_MT_6610">>        -> {terminal, '6610'};
                    <<"SERVICE_TS_MT_TRUELINK">>    -> {terminal, 'TRUELINK'};
                    <<"SERVICE_TS_MT_H323AUTH">>    -> {terminal, 'H323AUTH'};
                    <<"SERVICE_TS_MT_3610">>        -> {terminal, '3610'};
                    <<"SERVICE_TS_MT_HD1000">>      -> {terminal, 'HD1000'};
                    <<"SERVICE_TS_MT_HD100">>       -> {terminal, 'HD100'};
                    <<"SERVICE_TS_MT_HD100S">>      -> {terminal, 'HD100S'};
                    <<"SERVICE_TS_MT_WD1000">>      -> {terminal, 'WD1000'};
                    <<"SERVICE_TS_MT_IPHONE">>      -> {terminal, 'IPHONE'};
                    <<"SERVICE_TS_MT_ANDROID_PAD">>    -> {terminal, 'ANDROID_PAD'};
                    <<"SERVICE_TS_MT_ANDROID_PHONE">>  -> {terminal, 'ANDROID_PHONE'};
                    <<"SERVICE_TS_MT_WINDOWS_PAD">>    -> {terminal, 'WINDOWS_PAD'};

                    %% TS服务器
                    <<"SERVICE_TS_SVR_MPCD">>       -> {logical, 'MPCD'};
                    <<"SERVICE_TS_SVR_MCU">>        -> {logical, 'MCU'};
                    <<"SERVICE_TS_SVR_MP">>         -> {logical, 'MP'};
                    <<"SERVICE_TS_SVR_PRS">>        -> {logical, 'PRS'};

                    <<"SERVICE_TS_SVR_SUS">>        -> {logical, 'SUS'};
                    <<"SERVICE_TS_SVR_SUSMGR">>     -> {logical, 'SUSMGR'};
                    <<"SERVICE_TS_SVR_APS">>        -> {logical, 'APS'};
                    <<"SERVICE_TS_SVR_NTS">>        -> {logical, 'NTS'};
                    <<"SERVICE_TS_SVR_LGS">>        -> {logical, 'LGS'};
                    <<"SERVICE_TS_SVR_PAS">>        -> {logical, 'PAS'};

                    <<"SUPER_SERVICE_3AS">>         -> {logical, '3AS'};
                    <<"SUPER_SERVICE_NMS_MANAGER">> -> {logical, 'NMS_MANAGER'};

                    %% 开源服务器
                    <<"SERVICE_TS_SVR_MOVISION">>   -> {logical, 'MOVISION'};
                    <<"SERVICE_TS_SVR_MOOOO">>      -> {logical, 'MOOOO'};
                    <<"SERVICE_TS_SRV_MYSQL">>      -> {logical, 'MYSQL'};
                    <<"SERVICE_TS_SRV_LBS">>        -> {logical, 'LBS'};
                    <<"SERVICE_TS_SRV_XNU">>        -> {logical, 'XNU'};
                    <<"SERVICE_TS_SRV_GFS">>        -> {logical, 'GFS'};
                    <<"SERVICE_TS_SRV_MEM">>        -> {logical, 'MEM'};
                    <<"SERVICE_TS_SRV_HAPROXY">>    -> {logical, 'HAPROXY'};
                    <<"SERVICE_TS_SRV_ZKS">>        -> {logical, 'ZKS'};
                    <<"SERVICE_TS_SRV_LVS">>        -> {logical, 'LVS'};
                    <<"SERVICE_TS_SRV_NGINX">>      -> {logical, 'NGINX'};
                    <<"SERVICE_TS_SRV_XMPP">>       -> {logical, 'XMPP'};
                    <<"SERVICE_TS_SRV_TOMCAT">>     -> {logical, 'TOMCAT'};

                    %% 物理服务器
                    <<"SERVICE_SRV_PHY">>           -> {physical};

                    %% 专用于 EV_DEV_OFFLINE 消息的特殊处理
                    <<"UNKNOWN">>                   -> {collector, offline};
                    %% 专用于 EV_COLLECTOR_HEARTBEAT 消息的特殊处理
                    <<"COLLECTOR">>                 -> {collector, heartbeat};

                    _                               -> crash_me_for_unknown_devtype
                end,
    String.


to_warning_code(EventID) when is_binary(EventID) ->
    Code = case EventID of
        <<"EV_PFMINFO_CPU">>     -> 2002;    %% cpu 高于阈值
        <<"EV_PFMINFO_MEM">>     -> 2003;    %% 内存高于阈值
        <<"EV_PFMINFO_DISK">>    -> 2018;    %% 磁盘空间不足
        <<"EV_PFMINFO_NETCARD">> -> 2019     %% 网卡流量过载
    end,
    Code;
to_warning_code(_) ->
    lager:warning("[nms_task_control] EventID must be of binary type!").

to_description(Code) when is_integer(Code) ->
    Desc = case Code of
        2002 -> "CPU overhead threshold";
        2003 -> "MEM overhead threshold";
        2018 -> "DISK space insufficient";
        2019 -> "NETCARD traffic overload";
        2013 -> "Packet Lost overhead 5%";
        2014 -> "Packet Lost overhead 10%"
    end,
    Desc;
to_description(_) ->
    lager:warning("[nms_task_control] Code must be of interger type!").



warning_job(WarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID) ->
    warning_job(WarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID,none).

warning_job(WarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID,CustomCode) ->

    %%io:format("~n=-=-=-=-=> 我是神一样的分隔线 <=-=-=-=-=~n~n", []),

    case CustomCode of
        none ->
            WarningCode = to_warning_code(EventID),
            CodeDesc = to_description(WarningCode);
        _ ->
            WarningCode = CustomCode,
            CodeDesc = to_description(WarningCode)
    end,

    %% 从 warning_code 表中获取详细信息
    %% 鉴于 warning_code 表中的内容是必定存在的，此处不做 case 分支处理
    [Id,Type,Code,Name,Level,Description,Suggestion] = 
        gen_server:call(MySQLTask, {get_warning_code_detail, WarningCode}, infinity),

    lager:info("[nms_task_control] 'SELECT * FROM warning_code WHERE code=~p' -- Success!~n", [WarningCode]),

    %%io:format("~n~s~n", [string:chars($-,36)]),
    %%io:format("warning code[~p] detail:~n", [WarningCode]),
    %%io:format("      Id:~p~n", [Id]),
    %%io:format("      Type:~p~n", [Type]),  %% 中文
    %%io:format("      Code:~p~n", [Code]),
    %%io:format("      Name:~p~n", [Name]),  %% 中文
    %%io:format("      Level:~p~n", [Level]),
    %%io:format("      Description:~p~n", [Description]),  %% 中文
    %%io:format("      Suggestion:~p~n", [Suggestion]),    %% 中文
    %%io:format("~n~s~n", [string:chars($-,36)]),

    %% 查看 warning_unrepaired 表中是否存在对应的告警条目
    WarningItem = gen_server:call(MySQLTask, {get_device_warning_by_code, DomainMoid, DevMoid, WarningCode}, infinity),

    %% 判定当前上报信息是否触发告警
    case WarningTriggered of
        true -> %% 触发告警            
            lager:notice("[nms_task_control] recv ~p and trigger ~p~n", [EventID,CodeDesc]),

            case WarningItem of
                {exist, _} ->
                    %% 若 warning_unrepaired 表中已经存在对应条目，则不用对该表做变更
                    lager:info("[nms_task_control] 'SELECT * FROM warning_unrepaired WHERE xx' -- Success! 
                        warning ALREADY exists, nothing need to do.");
                {non_exist, _} ->
                    %% 若 warning_unrepaired 表中不存在对应条目，则需要插入一条新数据
                    lager:info("[nms_task_control] 'SELECT * FROM warning_unrepaired WHERE xx' -- Success! 
                        warning NOT exists, need to insert."),
                    
                    %% 向 warning_unrepaired 中插入一条新数据
                    case gen_server:call(MySQLTask, {add_unrepaired_warning, DevMoid, ?P_SERVER, DomainMoid, 
                                WarningCode, binary_to_list(Level), binary_to_list(Description), StatisticTime}, infinity) of
                        {ok, success} ->
                            lager:info("[nms_task_control] 'INSERT INTO warning_unrepaired' -- Success!");
                        {error, Err1} ->
                            lager:warning("[nms_task_control] 'INSERT INTO warning_unrepaired' -- Failed! Error '~p'~n", 
                                [Err1])
                    end,

                    %% 向 warning_repair_statistic 表中插入一条新的告警产生统计数据
                    case gen_server:call(MySQLTask, {add_warning_repair_statistic, DomainMoid, 
                                DevMoid, WarningCode, ?WARN_ON, StatisticTime}, infinity) of
                         {ok, success} ->
                            lager:info("[nms_task_control] 'INSERT INTO warning_repair_statistic' Success! WARN_ON!");
                         {error, Err2} ->
                            lager:warning("[nms_task_control] 'INSERT INTO warning_repair_statistic' Failed! Error '~p'~n", 
                                [Err2])
                    end,

                    io:format("", []);

                pool_init_failed ->
                    lager:warning("[nms_task_control] Something wrong happended, get 'pool_init_failed'!!")
            end,

            %% 向 redis 表 p_server:devid:warning 中添加告警码
            %% 添加不存在的告警码将得到返回值 {ok,<<"1">>}
            %% 添加已存在的告警码将得到返回值 {ok,<<"0">>}
            %% 因为仅在对非 SET 进行操作才会得到错误，故此处不需要做错误判定 -- 目前写了错误分支
            case gen_server:call(RedisTask, 
                        {add_physical_server_warning, DevMoid, WarningCode}, infinity) of
                {ok, <<"0">>} ->
                    lager:info("[nms_task_control] 'SADD p_server:~p:warning ~p' -- Success! 
                        but WarningCode.~p ALREADY exist!~n", [DevMoid, WarningCode, WarningCode]);  
                {ok, _} ->
                    lager:info("[nms_task_control] 'SADD p_server:~p:warning ~p' -- Success!~n", 
                        [DevMoid, WarningCode]);  
                {error, Err3} ->
                    lager:warning("[nms_task_control] 'SADD p_server:~p:warning xx' -- Failed! Error '~p'~n", 
                        [DevMoid, Err3])
            end,

            io:format("", []);
        false -> %% 未触发告警
            lager:notice("[nms_task_control] recv ~p but not trigger ~p~n", [EventID, CodeDesc]),

            case WarningItem of
                {exist, [_,_,_,_,_,_,_,{datetime,{{Year,Month,Day},{Hour,Min,Sec}}},_]=UnRepairedWarning} ->
                    %% 若 warning_unrepaired 表中已经存在对应条目，则读出该条目信息
                    %% 因为变化的信息仅为 resolve_time ，故其他信息可以直接使用上报内容                                    
                    
                    lager:info("[nms_task_control] 'SELECT * FROM warning_unrepaired WHERE xx' -- Success! 
                        warning ALREADY exists, need to delete!~n"),
                    lager:debug("[nms_task_control] UnRepairedWarning =~n~p~n", [UnRepairedWarning]),

                    %% 构造起始时间 e.g. 2014-10-29 12:26:12
                    lager:debug("[nms_task_control] {Year,Month,Day},{Hour,Min,Sec} = {~p,~p,~p},{~p,~p,~p}~n", 
                        [Year,Month,Day,Hour,Min,Sec]),                                    
                    StartTime = integer_to_list(Year)++"-"++integer_to_list(Month)++"-"++integer_to_list(Day)++" "++
                                integer_to_list(Hour)++"-"++integer_to_list(Min)++"-"++integer_to_list(Sec),
                    lager:debug("---- StartTime ---- = ~p~n", [StartTime]),

                    %% 添加 resolve_time 后存入 warning_repaired 表中 -- 此处未对 insert 失败做处理，可能有 bug
                    case gen_server:call(MySQLTask, {add_repaired_warning, DevMoid, ?P_SERVER, DomainMoid, 
                                WarningCode, binary_to_list(Level), binary_to_list(Description), StartTime, 
                                StatisticTime}, infinity) of
                         {ok, success} ->
                            lager:info("[nms_task_control] 'INSERT INTO warning_repaired' -- Success!");
                         {error, Err4} ->
                            lager:warning("[nms_task_control] 'INSERT INTO warning_repaired' -- Failed! Error '~p'~n", 
                                [Err4])
                    end,

                    %% 再将 warning_unrepaired 表中的该条目删除
                    case gen_server:call(MySQLTask, {del_unrepaired_warning, DevMoid, DomainMoid, 
                                WarningCode}, infinity) of
                         {ok, success} ->
                            lager:info("[nms_task_control] 'DELETE FROM warning_unrepaired' -- Success!");
                         {error, Err5} ->
                            lager:warning("[nms_task_control] 'DELETE FROM warning_unrepaired' -- Failed! Error '~p'~n", 
                                [Err5])
                    end,

                    %% 向 warning_repair_statistic 表中插入一条新的告警修复统计数据
                    case gen_server:call(MySQLTask, {add_warning_repair_statistic, DomainMoid, DevMoid, 
                                WarningCode, ?WARN_OFF, StatisticTime}, infinity) of
                         {ok, success} ->
                            lager:info("[nms_task_control] 'INSERT INTO warning_repair_statistic' Success! WARN_OFF!");
                         {error, Err6} ->
                            lager:warning("[nms_task_control] 'INSERT INTO warning_repair_statistic' Failed! Error '~p'~n", 
                                [Err6])
                    end,

                    io:format("", []);
               {non_exist, _} ->
                    %% 若 warning_unrepaired 表中不存在对应条目，则不用对该表做变更
                    lager:info("[nms_task_control] 'SELECT * FROM warning_unrepaired WHERE xx' -- Success! 
                        CPU warning NOT exists, nothing need to do.");
                pool_init_failed ->
                    lager:warning("[nms_task_control] Something wrong happended, get 'pool_init_failed'!!")
            end,

            %% 从 redis 表 p_server:devid:warning 中删除告警码
            %% 删除不存在的告警码将得到返回值 {ok,<<"0">>}
            %% 删除存在的告警码将得到返回值 {ok,<<"1">>}
            %% 因为仅在对非 SET 进行操作才会得到错误，故此处不需要做错误判定
            case gen_server:call(RedisTask, {del_physical_server_warning, DevMoid, WarningCode}, infinity) of
                {ok, <<"0">>} ->
                    lager:info("[nms_task_control] 'SREM p_server:~p:warning ~p' -- Success! but 
                        WarningCode.~p NOT exist!~n", [DevMoid, WarningCode, WarningCode]);  
                {ok, _} ->
                    lager:info("[nms_task_control] 'SREM p_server:~p:warning ~p' -- Success!~n", 
                        [DevMoid, WarningCode]);  
                {error, Err7} ->
                    lager:warning("[nms_task_control] 'SREM p_server:~p:warning xx' Failed! Error '~p'~n", 
                        [DevMoid, Err7])
            end,

            io:format("", [])
    end,

    %%io:format("~n=-=-=-=-=> 我是神一样的分隔线 <=-=-=-=-=~n~n", []),

    io:format("", []).



physical_device_proc(JsonObj, RedisTask, MySQLTask) ->

    DevMoid_ = rfc4627:get_field(JsonObj, "devid", undefined),
    lager:info("  -->  DevMoid = ~p~n", [DevMoid_]),
    DevMoid = binary_to_list(DevMoid_),

    StatisticTime_ = rfc4627:get_field(JsonObj, "rpttime", undefined),
    lager:info("  -->  StatisticTime = ~p~n", [StatisticTime_]),            
    StatisticTime = binary_to_list(StatisticTime_),

    EventID = rfc4627:get_field(JsonObj, "eventid", undefined),
    lager:info("  -->  EventID = ~p~n", [EventID]),

    %% 通过 "devid" 在 redis 中查询当前物理设备所属的域 ID
    %% 这里可能需要做异常处理，若 RedisCon 没有建立则会返回 {error, no_connection}
    %% 若信息不存在则会返回 {error,<<"Key Error">>}
    { ok, {DevMoid_, DevGuid_, DomainMoid_, DevName, Location, IP} } = 
        gen_server:call(RedisTask, {get_physical_server_info, DevMoid_}, infinity),

    io:format("~n"),

    lager:info("  -->  DevGuid = ~p~n", [DevGuid_]),
    lager:info("  -->  DomainMoid = ~p~n", [DomainMoid_]),
    lager:info("  -->  DevName = ~p~n", [DevName]),
    lager:info("  -->  Location = ~p~n", [Location]),
    lager:info("  -->  IP = ~p~n", [IP]),

    DomainMoid = binary_to_list(DomainMoid_),
    %%DevGuid = binary_to_list(DevGuid_),

    io:format("~n"),

    %% 通过 "eventid" 判定为信息类型，更新 xxx_statistic 表的内容
    case EventID of
        <<"EV_PFMINFO_CPU">>     ->
            %% 消息举例
            %% {obj,[{"devid",<<"1.1.1">>},
            %%       {"devtype",<<"SERVICE_SRV_PHY">>},
            %%       {"rpttime",<<"2014-11-19/16:43:47">>},
            %%       {"eventid",<<"EV_PFMINFO_CPU">>}]}
            %%       {"cpuinfo",
            %%           {obj,[{"cpuusage",0},
            %%                 {"coreinfo",
            %%                     [{obj,[{"cpucore1",100}]},
            %%                      {obj,[{"cpucore2",10}]},
            %%                      {obj,[{"cpucore3",20}]},
            %%                      {obj,[{"cpucore4",30}]},
            %%                      {obj,[{"cpucore5",40}]},
            %%                      {obj,[{"cpucore6",50}]},
            %%                      {obj,[{"cpucore7",60}]},
            %%                      {obj,[{"cpucore8",70}]}]},
            %%                 {"cpucorecount",8}]}},

            CPUInfo   = rfc4627:get_field(JsonObj, "cpuinfo", undefined),
            CoreNum = rfc4627:get_field(CPUInfo, "cpucorecount", 0),
            CoreInfo  = rfc4627:get_field(CPUInfo, "coreinfo", undefined),

            %% 将每个 CPU 的使用率写入 cpu_statistic 统计表
            %%[begin
            %%    case gen_server:call(MySQLTask, {add_cpu_statistic, DomainMoid, DevMoid, list_to_integer(N), Cpu, 
            %%            StatisticTime}, infinity) of
            %%        {ok, success} ->
            %%            lager:info("[nms_task_control] 'INSERT INTO cpu_statistic' -- Success! Index:~p~n", [N]);
            %%        {error, CpuErr0} -> %% 重复使用可能会报错
            %%            io:format("[nms_task_control] 'INSERT INTO cpu_statistic' -- Failed! Error '~p'~n", [CpuErr0])
            %%    end
            %%end || {obj, [{"cpucore"++N, Cpu}]} <- CoreInfo],

            [begin
                gen_server:call(MySQLTask, {add_cpu_statistic, DomainMoid, DevMoid, list_to_integer(N), Cpu, 
                        StatisticTime}, infinity)
            end || {obj, [{"cpucore"++N, Cpu}]} <- CoreInfo],


            %% 查询保存 CPU 阈值信息的表 resource_limit
            Cpu_Threshold = case gen_server:call(RedisTask, {get_server_cpu_limit}, infinity) of
                {error, CpuErr1} ->
                    lager:warning("[nms_task_control] 'GET server_cpu_limit' -- Failed! Error '~p'~n", [CpuErr1]),
                    case gen_server:call(MySQLTask, {get_server_cpu_limit}, infinity) of
                        {ok, CpuValFromMySQL} ->
                            lager:info("[nms_task_control] 'SELECT s_cpu FROM resource_limit' -- Success! Value '~p'~n", 
                                [CpuValFromMySQL]),
                            CpuValFromMySQL;
                        _ ->
                            lager:warning("[nms_task_control] 'SELECT s_cpu FROM resource_limit' -- Failed! 
                                Use ~p by default!~n", [?CPU_THRESHOLD_DEFAULT]),
                            ?CPU_THRESHOLD_DEFAULT
                    end;
                {ok, CpuValFromRedis} ->
                    lager:info("[nms_task_control] 'GET server_cpu_limit' -- Success! Value(~p)~n", [CpuValFromRedis]),
                    CpuValFromRedis
            end,
            
            CpuPctList = [ Pct || {obj, [{_, Pct}]} <- CoreInfo],

            %% 判定当前多核 CPU 的单核使用率是否触发告警状态
            Predicate = fun(V) -> 
                            V >= list_to_integer(binary_to_list(Cpu_Threshold))
                        end,
            CpuWarningTriggered = lists:any(Predicate, CpuPctList),

%% ------------------------------------------------------------
            %% 未处理失败
            warning_job(CpuWarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID), 

%% ------------------------------------------------------------

            %% 保存当前多核 CPU 的平均使用率到 redis 表 p_server:devid:resource
            CpuAverage = lists:sum(CpuPctList) div CoreNum,     
            case gen_server:call(RedisTask, {update_physical_server_cpu_resource, 
                        binary_to_list(DevMoid_), CpuAverage}, infinity) of
                {error, CpuErr2} ->
                    lager:warning("[nms_task_control] 'HMSET p_server:~p:resource cpu xx' -- Failed! Error '~p'~n", 
                        [DevMoid, CpuErr2]);
                {ok, _} ->
                    lager:info("[nms_task_control] 'HMSET p_server:~p:resource cpu ~p' -- Success!~n", 
                        [DevMoid, CpuAverage])
            end,

            io:format("", []);

        <<"EV_PFMINFO_MEM">>     ->
            %% 消息举例
            %% {obj,[{"devid",<<"1.1.1">>},
            %%       {"devtype",<<"SERVICE_SRV_PHY">>},
            %%       {"rpttime",<<"2014-11-19/16:43:47">>},
            %%       {"eventid",<<"EV_PFMINFO_MEM">>},
            %%       {"meminfo",
            %%           {obj,[{"total",3645440},
            %%                 {"userate",22},
            %%                 {"used",2793860}]}}]}

            MemInfo   = rfc4627:get_field(JsonObj, "meminfo", undefined),
            MemTotal = rfc4627:get_field(MemInfo, "total", 0),
            MemUsePct  = rfc4627:get_field(MemInfo, "userate", 0),
            MemUsed  = rfc4627:get_field(MemInfo, "used", 0),

            lager:info("  -->  Total = ~p~n", [MemTotal]),
            lager:info("  -->  Userate = ~p~n", [MemUsePct]),
            lager:info("  -->  Used = ~p~n", [MemUsed]),

            io:format("~n"),

            case gen_server:call(MySQLTask, {add_memory_statistic, DomainMoid, DevMoid, MemUsePct, StatisticTime}, infinity) of
                {ok, success} ->
                    lager:info("[nms_task_control] 'INSERT INTO memory_statistic' -- Success!");
                {error, MemErr0} ->
                    lager:warning("[nms_task_control] 'INSERT INTO memory_statistic' -- Failed! Error '~p'~n", [MemErr0])
            end,

            %% 查询保存 Mem 阈值信息的表 resource_limit
            Mem_Threshold = case gen_server:call(RedisTask, {get_server_mem_limit}, infinity) of
                {error, MemErr1} ->
                    lager:warning("[nms_task_control] 'GET server_memory_limit' -- Failed! Error '~p'~n", [MemErr1]),
                    case gen_server:call(MySQLTask, {get_server_mem_limit}, infinity) of
                        {ok, MemValFromMySQL} ->
                            lager:info("[nms_task_control] 'SELECT s_memory FROM resource_limit' -- Success! Value '~p'~n", 
                                [MemValFromMySQL]),
                            MemValFromMySQL;
                        _ ->
                            lager:warning("[nms_task_control] 'SELECT s_memory FROM resource_limit' -- Failed! 
                                Use ~p by default!~n", [?MEM_THRESHOLD_DEFAULT]),
                            ?MEM_THRESHOLD_DEFAULT
                    end;
                {ok, MemValFromRedis} ->
                    lager:info("[nms_task_control] 'GET server_memory_limit' -- Success! Value(~p)~n", [MemValFromRedis]),
                    MemValFromRedis
            end,

            %% 判定当前 MEM 使用率是否触发告警状态
            MemWarningTriggered = MemUsePct >= list_to_integer(binary_to_list(Mem_Threshold)),

%% ------------------------------------------------------------
            %% 未处理失败
            warning_job(MemWarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID), 

%% ------------------------------------------------------------

            %% 更新 redis 表 p_server:devid:resource
            case gen_server:call(RedisTask, {update_physical_server_mem_resource, DevMoid, MemUsePct}, infinity) of
                {error, MemErr2} ->
                    lager:warning("[nms_task_control] 'HMSET p_server:~p:resource memory xx' -- Failed! Error '~p'~n", 
                        [DevMoid, MemErr2]);
                {ok, _} ->
                    lager:info("[nms_task_control] 'HMSET p_server:~p:resource memory ~p' -- Success!", 
                        [DevMoid, MemUsePct])
            end,

            io:format("", []);

        <<"EV_PFMINFO_DISK">>    ->
            %% {obj,[{"devid",<<"1.1.1">>},
            %%       {"devtype",<<"SERVICE_SRV_PHY">>},
            %%       {"rpttime",<<"2014-11-19/16:43:47">>},
            %%       {"eventid",<<"EV_PFMINFO_DISK">>}]}
            %%       {"diskinfo",
            %%           {obj,[{"total",940193556},
            %%                 {"userate",3},
            %%                 {"used",28479932}]}},

            DiskInfo   = rfc4627:get_field(JsonObj, "diskinfo", undefined),
            DiskTotal = rfc4627:get_field(DiskInfo, "total", 0),
            DiskUsePct  = rfc4627:get_field(DiskInfo, "userate", 0),
            DiskUsed  = rfc4627:get_field(DiskInfo, "used", 0),

            lager:info("  -->  Total = ~p~n", [DiskTotal]),
            lager:info("  -->  Userate = ~p~n", [DiskUsePct]),
            lager:info("  -->  Used = ~p~n", [DiskUsed]),

            io:format("~n"),

            case gen_server:call(MySQLTask, {add_disk_statistic, DomainMoid, DevMoid, DiskUsePct, StatisticTime}, infinity) of
                {ok, success} ->
                    lager:info("[nms_task_control] 'INSERT INTO disk_statistic' -- Success!");
                {error, DiskErr0} ->
                    lager:warning("[nms_task_control] 'INSERT INTO disk_statistic' -- Failed! Error '~p'~n", [DiskErr0])
            end,

            %% 查询保存 Disk 阈值信息的表 resource_limit
            Disk_Threshold = case gen_server:call(RedisTask, {get_server_disk_limit}, infinity) of
                {error, DiskErr1} ->
                    lager:warning("[nms_task_control] 'GET server_disk_limit' -- Failed! Error '~p'~n", [DiskErr1]),
                    case gen_server:call(MySQLTask, {get_server_disk_limit}, infinity) of
                        {ok, DiskValFromMySQL} ->
                            lager:info("[nms_task_control] 'SELECT s_disk FROM resource_limit' -- Success! Value '~p'~n", 
                                [DiskValFromMySQL]),
                            DiskValFromMySQL;
                        _ ->
                            lager:warning("[nms_task_control] 'SELECT s_disk FROM resource_limit' -- Failed! 
                                Use ~p by default!~n", [?DISK_THRESHOLD_DEFAULT]),
                            ?DISK_THRESHOLD_DEFAULT
                    end;
                {ok, DiskValFromRedis} ->
                    lager:info("[nms_task_control] 'GET server_disk_limit' -- Success! Value(~p)~n", [DiskValFromRedis]),
                    DiskValFromRedis
            end,

            %% 判定当前 Disk 使用率是否触发告警状态
            DiskWarningTriggered = DiskUsePct >= list_to_integer(binary_to_list(Disk_Threshold)),

%% ------------------------------------------------------------
            %% 未处理失败
            warning_job(DiskWarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID), 

%% ------------------------------------------------------------

            %% 更新 redis 表 p_server:devid:resource
            case gen_server:call(RedisTask, {update_physical_server_disk_resource, DevMoid, DiskUsePct}, infinity) of
                {error, DiskErr2} ->
                    lager:warning("[nms_task_control] 'HMSET p_server:~p:resource disk xx' -- Failed! Error '~p'~n", 
                        [DevMoid, DiskErr2]);
                {ok, _} ->
                    lager:info("[nms_task_control] 'HMSET p_server:~p:resource disk ~p' -- Success!", 
                        [DevMoid, DiskUsePct])
            end,

            io:format("", []);

        <<"EV_PFMINFO_NETCARD">> ->
            %% {obj,[{"devid",<<"1.1.1">>},
            %%       {"devtype",<<"SERVICE_SRV_PHY">>},
            %%       {"rpttime",<<"2014-11-21/9:41:10">>},
            %%       {"eventid",<<"EV_PFMINFO_NETCARD">>},
            %%       {"netcardinfo",
            %%           {obj,[{"cardcount",2},
            %%                 {"recvpktloserate",0},
            %%                 {"recvbytes",0},
            %%                 {"sendbytes",0},
            %%                 {"netcards",
            %%                     [{obj,[{"netcard1",
            %%                                {obj,[{"recvpktloserate",0},
            %%                                      {"sendbytes",0},
            %%                                      {"recvbytes",0}]}}]},
            %%                      {obj,[{"netcard2",
            %%                                {obj,[{"recvpktloserate",0},
            %%                                      {"sendbytes",0},
            %%                                      {"recvbytes",0}]}}]}]}]}}]}

            NetCardInfo   = rfc4627:get_field(JsonObj, "netcardinfo", undefined),
            CardNum = rfc4627:get_field(NetCardInfo, "cardcount", 0),
            RecvLostPctTotal  = rfc4627:get_field(NetCardInfo, "recvpktloserate", 0),
            RecvBytesTotal  = rfc4627:get_field(NetCardInfo, "recvbytes", 0),
            SendBytesTotal  = rfc4627:get_field(NetCardInfo, "sendbytes", 0),
            Netcards  = rfc4627:get_field(NetCardInfo, "netcards", 0),

            lager:info("  -->  cardcount = ~p~n", [CardNum]),
            lager:info("  -->  total recvpktloserate = ~p~n", [RecvLostPctTotal]),
            lager:info("  -->  total recvbytes = ~p~n", [RecvBytesTotal]),
            lager:info("  -->  total sendbytes = ~p~n", [SendBytesTotal]),

            io:format("~n"),

            %% 将每个 Netcard 上的发送/接收流量写入 netcard_statistic 统计表
            %%[begin
            %%    case gen_server:call(MySQLTask, {add_net_statistic, DomainMoid, DevMoid, list_to_integer(Index), 
            %%            RecvBytes, SendBytes, StatisticTime}, infinity) of
            %%        {ok, success} ->
            %%            lager:info("[nms_task_control] 'INSERT INTO netcard_statistic' -- Success! Index:~p~n", [Index]);
            %%        {error, NetErr0} -> %% 重复使用可能会报错
            %%            io:format("[nms_task_control] 'INSERT INTO netcard_statistic' -- Failed! Error '~p'~n", [NetErr0])
            %%    end
            %%end || {obj,[{"netcard"++Index,
            %%                 {obj,[{"recvpktloserate",_RecvLostPct},
            %%                       {"sendbytes",SendBytes},
            %%                       {"recvbytes",RecvBytes}]}}]} <- Netcards],

            [begin
                gen_server:call(MySQLTask, {add_net_statistic, DomainMoid, DevMoid, list_to_integer(Index), 
                        RecvBytes, SendBytes, StatisticTime}, infinity)
            end || {obj,[{"netcard"++Index,
                             {obj,[{"recvpktloserate",_RecvLostPct},
                                   {"sendbytes",SendBytes},
                                   {"recvbytes",RecvBytes}]}}]} <- Netcards],                                           

            %% 查询 Redis 保存 发送/接收流量 阈值信息的表 resource_limit
            Net_Threshold = case gen_server:call(RedisTask, {get_server_net_limit}, infinity) of
                {error, NetErr1} ->
                    lager:warning("[nms_task_control] 'GET server_port_limit' -- Failed! Error '~p'~n", [NetErr1]),
                    case gen_server:call(MySQLTask, {get_server_net_limit}, infinity) of
                        {ok, NetValFromMySQL} ->
                            lager:info("[nms_task_control] 'SELECT s_port FROM resource_limit' -- Success! Value '~p'~n", 
                                [NetValFromMySQL]),
                            NetValFromMySQL;
                        _ ->
                            lager:warning("[nms_task_control] 'SELECT s_port FROM resource_limit' -- Failed! 
                                Use ~p by default!~n", [?NET_THRESHOLD_DEFAULT]),
                            ?NET_THRESHOLD_DEFAULT
                    end;
                {ok, NetValFromRedis} ->
                    lager:info("[nms_task_control] 'GET server_port_limit' -- Success! Value(~p)~n", [NetValFromRedis]),
                    NetValFromRedis
            end,

            %% 判定当前多 NetCard 网卡设备上单 NetCard 流量是否触发告警状态 - 不区分收发
            NetTrafficList_ = [ [S,R] || {obj,[{_,{obj,[{_,_},{"sendbytes",S},{"recvbytes",R}]}}]} <- Netcards],
            NetTrafficList = lists:flatten(NetTrafficList_),
            Predicate = fun(V) -> 
                            V >= list_to_integer(binary_to_list(Net_Threshold))
                        end,
            NetTrafficWarningTriggered = lists:any(Predicate, NetTrafficList),

%% ------------------------------------------------------------
            %% 未处理失败
            warning_job(NetTrafficWarningTriggered,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID), 
%% ------------------------------------------------------------

            %% 保存 RecvBytesTotal 和 SendBytesTotal 值到 redis 表 p_server:devid:resource
            case gen_server:call(RedisTask, {update_physical_server_net_resource, DevMoid, RecvBytesTotal, SendBytesTotal}, 
                    infinity) of
                {error, DiskErr2} ->
                    lager:warning("[nms_task_control] 'HMSET p_server:~p:resource portin xx portout xx' -- Failed! Error '~p'~n", 
                        [DevMoid, DiskErr2]);
                {ok, _} ->
                    lager:info("[nms_task_control] 'HMSET p_server:~p:resource portin ~p portout ~p' -- Success!~n", 
                        [DevMoid, RecvBytesTotal, SendBytesTotal])
            end,


            %% 判定当前多 NetCard 网卡设备上单 NetCard 接收丢包率是否触发告警状态
            PacketLostPctList = [ [PacketLostPct] || {obj,[{_,{obj,[{"recvpktloserate",PacketLostPct},_,_]}}]} <- Netcards],

            %% 判定否超过 5% 和 10% -- 分别对应告警码 2013 和 2014
            Fun10 = fun(V10) -> V10 >= 10 end,                    
            Pct10 = lists:any(Fun10, PacketLostPctList),

%% ------------------------------------------------------------
            %% 未处理失败
            warning_job(Pct10,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID,2014),
%% ------------------------------------------------------------ 

            Fun5 = fun(V5) -> V5 >= 5 end,
            Pct5 = lists:any(Fun5, PacketLostPctList),

%% ------------------------------------------------------------
            %% 未处理失败
            warning_job(Pct5,MySQLTask,RedisTask,DevMoid,DomainMoid,StatisticTime,EventID,2013),     
%% ------------------------------------------------------------               

            io:format("", [])
    end.


terminal_device_proc(_JsonObj, _RedisTask, _MySQLTask) ->
    void.

logical_device_proc(_JsonObj, _RedisTask, _MySQLTask) ->
    void.

collector_offline_proc(_JsonObj, _RedisTask, _MySQLTask) ->
    void.

collector_heartbeat_proc(_JsonObj, _RedisTask, _MySQLTask) ->
    void.


%% 此函数内部均没有进行异常处理，若 rfc4627:get_field 返回 undefined 则崩溃
msg_parser(JsonObj, #state{redis_task=RedisTask, mysql_task=MySQLTask}) ->

    Devtype = rfc4627:get_field(JsonObj, "devtype", undefined),

    case devtype_distinguish(Devtype) of
        {physical} ->
            lager:info("Devtype => {physical}~n", []),
            lager:notice("<=============== PHYSICAL DEVICE ===============>"),
            physical_device_proc(JsonObj, RedisTask, MySQLTask),
            lager:notice("<=============== PHYSICAL DEVICE ===============>"),
            io:format("", []);
        {terminal, Type} ->
            lager:info("Devtype => {terminal, ~p}~n", [Type]),
            lager:notice("<=============== TERMINAL DEVICE ===============>"),
            terminal_device_proc(JsonObj, RedisTask, MySQLTask),
            lager:notice("<=============== TERMINAL DEVICE ===============>"),
            io:format("", []);
        {logical, Type}  ->
            lager:info("Devtype => {logical, ~p}~n", [Type]),
            lager:notice("<=============== LOGICAL DEVICE ===============>"),
            logical_device_proc(JsonObj, RedisTask, MySQLTask),
            lager:notice("<=============== LOGICAL DEVICE ===============>"),
            io:format("", []);
        {collector, offline}  ->
            lager:info("Devtype => {collector, offline}~n", []),
            lager:notice("<=============== COLLECTOR OFFLINE ===============>"),
            collector_offline_proc(JsonObj, RedisTask, MySQLTask),
            lager:notice("<=============== COLLECTOR OFFLINE ===============>"),
            io:format("", []);
        {collector, heartbeat}  ->
            lager:info("Devtype => {collector, heartbeat}~n", []),
            lager:notice("<=============== COLLECTOR HEARTBEAT ===============>"),
            collector_heartbeat_proc(JsonObj, RedisTask, MySQLTask),
            lager:notice("<=============== COLLECTOR HEARTBEAT ===============>"),
            io:format("", []);
        Err ->
            lager:error("Recv UNKNOWN Devtype: ~p~n", [Devtype]),
            throw(Err)
    end.


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([TRef, MQTask, RedisTask, MySQLTask]) ->
    ok = nms_config:set_task_control(TRef, self()),
    io:format("===>  nms_config:nms_task_control = [~p,~p]~n", [TRef, self()]),
    {ok, #state{tref=TRef, mq_task=MQTask, redis_task=RedisTask, mysql_task=MySQLTask}}.

handle_call( {do_consume, QueueN}, _From, #state{mq_task=MQTask} = State) ->
    _ = nms_rabbitmq_task:do_consume(MQTask, self(), QueueN),
    lager:notice("[nms_task_control] handle_call/3 Recv {do_consume, ~p}~n", [QueueN]),
    Reply = ok,
    {reply, Reply, State};
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
            %%io:format("Result=~p~n~nRemainder=~p~n~n", [Result, Remainder]),
            lager:info("~n~nResult=~p~n~nRemainder=~p~n~n", [Result, Remainder]),
            lager:notice("#####  rfc4627:decode  #####"),
            msg_parser(Result, State);

        {error, Error} ->
            lager:error("after rfc4627:decode   --> ~nerror=[~p]~n", [Error])
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
