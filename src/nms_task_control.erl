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

                    %% 5.0 新命名
                    <<"Skywalker 100">>             ->{terminal, 'Skywalker 100'};
                    <<"Skywalker X100">>            ->{terminal, 'Skywalker X100'};
                    <<"Skywalker 300">>             ->{terminal, 'Skywalker 300'};
                    <<"Skywalker X300">>            ->{terminal, 'Skywalker X300'};
                    <<"Skywalker X500">>            ->{terminal, 'Skywalker X500'};
                    <<"Skywalker X700">>            ->{terminal, 'Skywalker X700'};
                    <<"Skywalker X900">>            ->{terminal, 'Skywalker X900'};
                    <<"Skywalker for Windows">>     ->{terminal, 'Skywalker for Windows'};
                    <<"Skywalker for iPhone">>      ->{terminal, 'Skywalker for iPhone'};
                    <<"Skywalker for iPad">>        ->{terminal, 'Skywalker for iPad'};
                    <<"Skywalker for Android">>     ->{terminal, 'Skywalker for Android'};
                    <<"Skywalker for Mac">>         ->{terminal, 'Skywalker for Mac'};
                    <<"Skywalker for Android TV">>  ->{terminal, 'Skywalker for Android TV'};

                    %% TS服务器
                    <<"SERVICE_TS_SRV_MPCD">>       -> {logical, 'MPCD'};
                    <<"SERVICE_TS_SRV_MCU">>        -> {logical, 'MCU'};
                    <<"SERVICE_TS_SRV_MP">>         -> {logical, 'MP'};
                    <<"SERVICE_TS_SRV_PRS">>        -> {logical, 'PRS'};

                    <<"SERVICE_TS_SRV_SUS">>        -> {logical, 'SUS'};
                    <<"SERVICE_TS_SRV_SUSMGR">>     -> {logical, 'SUSMGR'};
                    <<"SERVICE_TS_SRV_APS">>        -> {logical, 'APS'};
                    <<"SERVICE_TS_SRV_NTS">>        -> {logical, 'NTS'};
                    <<"SERVICE_TS_SRV_LGS">>        -> {logical, 'LGS'};
                    <<"SERVICE_TS_SRV_PAS">>        -> {logical, 'PAS'};

                    <<"SUPER_SERVICE_3AS">>         -> {logical, '3AS'};
                    <<"SUPER_SERVICE_NMS_MANAGER">> -> {logical, 'NMS_MANAGER'};

                    %% 开源服务器
                    <<"SERVICE_TS_SRV_MOVISION">>   -> {logical, 'MOVISION'};
                    <<"SERVICE_TS_SRV_MOOOO">>      -> {logical, 'MOOOO'};
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

                    %% 核心域服务
                    <<"BMC">>       -> {logical, 'BMC'};  %% Business Management Console
                    <<"AMC">>       -> {logical, 'AMC'};  %% Account Management Console
                    <<"UPS">>       -> {logical, 'UPS'};  %% Unified Presence Server

                    %% 平台域服务
                    <<"APS">>       -> {logical, 'APS'};   %% Acess Point Server
                    <<"PAS">>       -> {logical, 'PAS'};   %% Protocol Access Server
                    %% 不区分核心域和平台域 UPS
                    %%<<"UPS">>       -> {logical, 'UPS'};   %% Unified Presence Server
                    <<"MPCD">>      -> {logical, 'MPCD'};   %% MPCD
                    <<"CSS">>       -> {logical, 'CSS'};    %% Conference Schedule Server
                    <<"CMS">>       -> {logical, 'CMS'};    %% Conference Management Server
                    <<"MRS">>       -> {logical, 'MRS'};    %% Media Relay Server
                    <<"MPS">>       -> {logical, 'MPS'};    %% Media Processing Server
                    <<"IVR">>       -> {logical, 'IVR'};    %% Interactive  Voice Response
                    <<"CMC">>       -> {logical, 'CMC'};    %% Conference Management Console
                    <<"TVS">>       -> {logical, 'TVS'};    %% TV-WALL Server
                    <<"XMPP">>      -> {logical, 'XMPP'};   %% The Extensible Messaging and Presence Protocol
                    <<"SNS">>       -> {logical, 'SNS'};    %% Social Networking Services
                    <<"LGS">>       -> {logical, 'LGS'};    %% Log Server
                    <<"NTP">>       -> {logical, 'NTP'};    %% Network Time Protocol
                    <<"SUS">>       -> {logical, 'SUS'};    %% Software Update Server
                    <<"SUSMGR">>    -> {logical, 'SUSMGR'}; %% SUS Manager
                    <<"SDS">>       -> {logical, 'SDS'};    %% Scalable Deployment Serve
                    <<"NDS">>       -> {logical, 'NDS'};    %% Network Detect Server
                    <<"LBS">>       -> {logical, 'LBS'};    %% Load Balance Server
                    <<"DRM">>       -> {logical, 'DRM'};    %% Dynamic Resource Management
                    <<"ZK">>        -> {logical, 'ZK'};     %% Zookeeper
                    <<"MQ">>        -> {logical, 'MQ'};     %% MQ
                    <<"REDIS">>     -> {logical, 'REDIS'};  %% REDIS

                    %% 机框
                    <<"UMU">>       -> {logical, 'UMU'};   %% Unified Management Unit
                    <<"CEU">>       -> {logical, 'CEU'};   %% Chief Executive Unit
                    <<"XMPU">>      -> {logical, 'XMPU'};  %% Extended Media Processing Unit
                    <<"SMU">>       -> {logical, 'SMU'};   %% Station Management Unit

                    %% 外围服务器
                    <<"VRS">>       -> {logical, 'VRS'};   %% Video Recording System
                    <<"DCS">>       -> {logical, 'DCS'};   %% Data Conferencing System
                    <<"AGW">>       -> {logical, 'AGW'};   %% Audio Gateway
                    <<"BGW">>       -> {logical, 'BGW'};   %% Border Gateway

                    %% 物理服务器
                    <<"Server">>    -> {physical};         %% Physical Server

                    %% 物理服务器
                    <<"SERVICE_SRV_PHY">>           -> {physical};

                    %% 专用于 EV_COLLECTOR_HEARTBEAT 消息的特殊处理
                    <<"COLLECTOR">>                 -> {collector, heartbeat};

                    _                               -> crash_me_for_unknown_devtype
                end,
    String.


to_description(Code) when is_integer(Code) ->
    Desc = case Code of
        %% 终端告警
        1001 -> "Terminal CPU overhead threshold";
        1002 -> "Terminal online/offline";
        1003 -> "Terminal Packet Lost overhead 5%";
        1004 -> "Terminal Packet Lost overhead 10%";
        1005 -> "Terminal 1st Video Source Lost";
        1006 -> "Terminal Register GK Failure";
        1007 -> "Terminal Version too Old";
        1008 -> "Terminal Reboot Abnormally";
        1009 -> "Terminal MEM overhead threshold";
        1010 -> "Terminal FileSystem Fault";
        1011 -> "Terminal Task Busy";
        1016 -> "Terminal CPUFan Fault";
        1017 -> "Terminal Temperature too High";
        1018 -> "Terminal Voltage too High";
        1020 -> "Terminal 2nd Video Source Lost";
        1021 -> "Terminal 3rd Video Source Lost";
        1022 -> "Terminal 4th Video Source Lost";
        1023 -> "Terminal 5th Video Source Lost";
        1024 -> "Terminal 6th Video Source Lost";
        1025 -> "Terminal 7th Video Source Lost";
        1026 -> "Terminal 8th Video Source Lost";
        1027 -> "Terminal 9th Video Source Lost";
        1028 -> "Terminal 10th Video Source Lost";

        %% 服务器告警
        2001 -> "BMC Connection Failure";
        2002 -> "Server CPU overhead threshold";
        2003 -> "Server MEM overhead threshold";
        2004 -> "Server NTP Synchronization Failure";
        2005 -> "PAS Access Overload";
        2006 -> "PAS CallPair Overhead";
        2007 -> "MPS Media Source Overhead";
        2011 -> "Server Process Crash";
        2012 -> "Server Version too Old";
        2013 -> "MPS RecvPacket Lost overhead 5%";
        2014 -> "MPS RecvPacket Lost overhead 10%";
        2015 -> "Server online/offline";
        2016 -> "NMA Access Overload";
        2018 -> "Server DISK space insufficient";
        2019 -> "Server NETCARD traffic overload";

        %% MCU 告警
        3001 -> "MCU Reboot";
        3002 -> "MCU PowerBoard Failure";
        3005 -> "MCU one Board not Register";
        3006 -> "MCU one Board Configuration Error";
        3007 -> "MCU BoxFan Fault";
        3008 -> "MCU one Board Temperature too High";
        3009 -> "MCU CPU overhead threshold";
        3010 -> "MCU MEM overhead threshold";
        3011 -> "MCU PowerSupplyFan Fault";
        3012 -> "MCU CPU Temperature too High";
        3013 -> "MCU CPUFan Fault";
        3014 -> "MCU Task Busy";
        3015 -> "MCU DISK space insufficient"
    end,
    Desc;
to_description(_) ->
    lager:warning("[nms_task_control] Code must be of interger type!").

channelid_to_warningcode(VideoSrcChannelID) ->
    WarningCode = case VideoSrcChannelID of
        0 -> 1005;  %% 终端侧上报消息中 0 代表通道 1
        1 -> 1020;
        2 -> 1021;
        3 -> 1022;
        4 -> 1023;
        5 -> 1024;
        6 -> 1025;
        7 -> 1026;
        8 -> 1027;
        9 -> 1028;
        _ -> throw(out_of_current_range)
    end,
    WarningCode.



update_mysql_warning_info(WarningTriggered,MySQLTask,DevMoid,DomainMoid,DevType,StatisticTime,EventID,WarningCode) ->

    %%io:format("~n=-=-=-=-=> 我是神一样的分隔线 <=-=-=-=-=~n~n", []),

    CodeDesc = to_description(WarningCode),

    %% 从 warning_code 表中获取详细信息
    %% 鉴于 warning_code 表中的内容是必定存在的，此处不做 case 分支处理
    [_Id,_Type,_Code,_Name,Level,Description,_Suggestion] = 
        gen_server:call(MySQLTask, {get_warning_code_detail, WarningCode}, infinity),

    lager:info("[nms_task_control] 'SELECT * FROM warning_code WHERE code=~p' -- Success!~n", [WarningCode]),

    %%io:format("~n~s~n", [string:chars($-,36)]),
    %%io:format("warning code[~p] detail:~n", [WarningCode]),
    %%io:format("      Id:~p~n", [Id]),
    %%io:format("      Type:~ts~n", [Type]),  %% 中文
    %%io:format("      Code:~p~n", [Code]),
    %%io:format("      Name:~ts~n", [Name]),  %% 中文
    %%io:format("      Level:~p~n", [Level]),
    %%io:format("      Description:~ts~n", [Description]),  %% 中文
    %%io:format("      Suggestion:~ts~n", [Suggestion]),    %% 中文
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
                    case gen_server:call(MySQLTask, {add_unrepaired_warning, DevMoid, DevType, DomainMoid, 
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

            io:format("", []);
        false -> %% 未触发告警
            lager:notice("[nms_task_control] recv ~p but not trigger ~p~n", [EventID, CodeDesc]),

            case WarningItem of
                {exist, [_,_,_,_,_,_,_,{datetime,{{Year,Month,Day},{Hour,Min,Sec}}},_]=UnRepairedWarning} ->
                    %% 若 warning_unrepaired 表中已经存在对应条目，则读出该条目信息
                    %% 因为变化的信息仅为 resolve_time ，故其他信息可以直接使用上报内容                                    
                    
                    lager:info("[nms_task_control] 'SELECT * FROM warning_unrepaired WHERE xx' -- Success! 
                        warning ALREADY exists, need to delete!"),
                    lager:debug("[nms_task_control] UnRepairedWarning =~n~p~n", [UnRepairedWarning]),

                    %% 构造起始时间 e.g. 2014-10-29 12:26:12
                    lager:debug("[nms_task_control] {Year,Month,Day},{Hour,Min,Sec} = {~p,~p,~p},{~p,~p,~p}~n", 
                        [Year,Month,Day,Hour,Min,Sec]),                                    
                    StartTime = integer_to_list(Year)++"-"++integer_to_list(Month)++"-"++integer_to_list(Day)++" "++
                                integer_to_list(Hour)++"-"++integer_to_list(Min)++"-"++integer_to_list(Sec),
                    lager:debug("---- StartTime ---- = ~p~n", [StartTime]),

                    %% 添加 resolve_time 后存入 warning_repaired 表中 -- 此处未对 insert 失败做处理，可能有 bug
                    case gen_server:call(MySQLTask, {add_repaired_warning, DevMoid, DevType, DomainMoid, 
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
                        warning NOT exists, nothing need to do.");
                pool_init_failed ->
                    lager:warning("[nms_task_control] Something wrong happended, get 'pool_init_failed'!!")
            end,

            io:format("", [])
    end,

    %%io:format("~n=-=-=-=-=> 我是神一样的分隔线 <=-=-=-=-=~n~n", []),

    io:format("", []).


update_redis_warning_info(WarningTriggered,RedisTask,DevMoid,DevType,WarningCode) ->

    case WarningTriggered of
        true -> %% 触发告警
            case DevType of
                ?P_SERVER ->
                    Tag = 'p_server',
                    Handler = add_physical_server_warning;
                ?L_SERVER ->
                    Tag = 'l_server',
                    Handler = add_logical_server_warning;
                ?TERMINAL ->
                    Tag = 'terminal',
                    Handler = add_terminal_warning
            end,
            Action = 'SADD',
            Desc   = 'ALREADY';
            
        false -> %% 未触发告警
            case DevType of
                ?P_SERVER ->
                    Tag = 'p_server',
                    Handler = del_physical_server_warning;
                ?L_SERVER ->
                    Tag = 'l_server',
                    Handler = del_logical_server_warning;
                ?TERMINAL ->
                    Tag = 'terminal',
                    Handler = del_terminal_warning
            end,
            Action = 'SREM',
            Desc   = 'NOT'
    end,

    case gen_server:call(RedisTask, {Handler, DevMoid, WarningCode}, infinity) of
        {ok, <<"0">>} ->
            lager:info("[nms_task_control] '~p ~p:~p:warning ~p' -- Success! 
                but WarningCode.~p ~p exist!~n", [Action,Tag,DevMoid,WarningCode,WarningCode,Desc]);
        {ok, _} ->
            lager:info("[nms_task_control] '~p ~p:~p:warning ~p' -- Success!~n", 
                [Action, Tag, DevMoid, WarningCode]);  
        {error, Err} ->
            lager:warning("[nms_task_control] '~p ~p:~p:warning xx' -- Failed! Error '~p'~n", 
                [DevMoid, Tag, Err])
    end,

    io:format("", []).


sadd_chan_index(RedisTask, DevMoid, privideo_send_chan, PriVideoSendChanList) ->
    lager:info("[nms_task_control] PriVideoSend Channel = ~p~n", [PriVideoSendChanList]),
    case gen_server:call(RedisTask, 
            {add_terminal_meeting_channel, DevMoid, privideo_send_chan, PriVideoSendChanList}, infinity) of
        {error, ChanErr} ->
            lager:warning("[nms_task_control] 'SADD terminal:~p:meetingdetail:privideo_send_chan' -- Failed! Error '~p'~n", 
                [DevMoid, ChanErr]);
        _ ->
            lager:info("[nms_task_control] 'SADD terminal:~p:meetingdetail:privideo_send_chan' -- Success!~n", 
                [DevMoid])
    end;

sadd_chan_index(RedisTask, DevMoid, privideo_recv_chan, PriVideoRecvChanList) ->
    lager:info("[nms_task_control] PriVideoRecv Channel = ~p~n", [PriVideoRecvChanList]),
    case gen_server:call(RedisTask, 
            {add_terminal_meeting_channel, DevMoid, privideo_recv_chan, PriVideoRecvChanList}, infinity) of
        {error, ChanErr} ->
            lager:warning("[nms_task_control] 'SADD terminal:~p:meetingdetail:privideo_recv_chan' -- Failed! Error '~p'~n", 
                [DevMoid, ChanErr]);
        _ ->
            lager:info("[nms_task_control] 'SADD terminal:~p:meetingdetail:privideo_recv_chan' -- Success!~n", 
                [DevMoid])
    end;

sadd_chan_index(RedisTask, DevMoid, assvideo_send_chan, AssVideoSendChanList) ->
    lager:info("[nms_task_control] AssVideoSend Channel = ~p~n", [AssVideoSendChanList]),
    case gen_server:call(RedisTask, 
            {add_terminal_meeting_channel, DevMoid, assvideo_send_chan, AssVideoSendChanList}, infinity) of
        {error, ChanErr} ->
            lager:warning("[nms_task_control] 'SADD terminal:~p:meetingdetail:assvideo_send_chan' -- Failed! Error '~p'~n", 
                [DevMoid, ChanErr]);
        _ ->
            lager:info("[nms_task_control] 'SADD terminal:~p:meetingdetail:assvideo_send_chan' -- Success!~n", 
                [DevMoid])
    end;

sadd_chan_index(RedisTask, DevMoid, assvideo_recv_chan, AssVideoRecvChanList) ->
    lager:info("[nms_task_control] AssVideoRecv Channel = ~p~n", [AssVideoRecvChanList]),
    case gen_server:call(RedisTask, 
            {add_terminal_meeting_channel, DevMoid, assvideo_recv_chan, AssVideoRecvChanList}, infinity) of
        {error, ChanErr} ->
            lager:warning("[nms_task_control] 'SADD terminal:~p:meetingdetail:assvideo_recv_chan' -- Failed! Error '~p'~n", 
                [DevMoid, ChanErr]);
        _ ->
            lager:info("[nms_task_control] 'SADD terminal:~p:meetingdetail:assvideo_recv_chan' -- Success!~n", 
                [DevMoid])
    end;

sadd_chan_index(RedisTask, DevMoid, audio_send_chan, AudioSendChanList) ->
    lager:info("[nms_task_control] AudioSend Channel = ~p~n", [AudioSendChanList]),
    case gen_server:call(RedisTask, 
            {add_terminal_meeting_channel, DevMoid, audio_send_chan, AudioSendChanList}, infinity) of
        {error, ChanErr} ->
            lager:warning("[nms_task_control] 'SADD terminal:~p:meetingdetail:audio_send_chan' -- Failed! Error '~p'~n", 
                [DevMoid, ChanErr]);
        _ ->
            lager:info("[nms_task_control] 'SADD terminal:~p:meetingdetail:audio_send_chan' -- Success!~n", 
                [DevMoid])
    end;

sadd_chan_index(RedisTask, DevMoid, audio_recv_chan, AudioRecvChanList) ->
    lager:info("[nms_task_control] AudioRecv Channel = ~p~n", [AudioRecvChanList]),
    case gen_server:call(RedisTask, 
            {add_terminal_meeting_channel, DevMoid, audio_recv_chan, AudioRecvChanList}, infinity) of
        {error, ChanErr} ->
            lager:warning("[nms_task_control] 'SADD terminal:~p:meetingdetail:audio_recv_chan' -- Failed! Error '~p'~n", 
                [DevMoid, ChanErr]);
        _ ->
            lager:info("[nms_task_control] 'SADD terminal:~p:meetingdetail:audio_recv_chan' -- Success!~n", 
                [DevMoid])
    end.

audio_format_trans(Format) when is_integer(Format) ->
    case Format of
        0 -> "emAG711a";
        1 -> "emAG711u";
        2 -> "emAG722";
        3 -> "emAG7231";
        4 -> "emAG728";
        5 -> "emAG729";
        6 -> "emAMP3";
        7 -> "emAG721";
        8 -> "emAG7221";
        9 -> "emAG719";
        10 -> "emAMpegAACLC";
        11 -> "emAMpegAACLD";
        12 -> "emAOpus";
        _ -> throw(out_of_audio_format_range)
    end;
audio_format_trans(_) ->
    lager:error("[nms_task_control] audio Format is not integer type!"),
    throw(format_is_wrong_type).

video_format_trans(Format) when is_integer(Format) ->
    case Format of
        0 -> "emVH261";
        1 -> "emVH262";
        2 -> "emVH263";
        3 -> "emVH263plus";
        4 -> "emVH264";
        5 -> "emVMPEG4";
        6 -> "emVH265";
        _ -> throw(out_of_video_format_range)
    end;
video_format_trans(_) ->
    lager:error("[nms_task_control] video Format is not integer type!"),
    throw(format_is_wrong_type).


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

update_timer_by_collectorid(RedisTask, CollectorID) ->

    %% 根据 collectorid 从 Redis 表 collector:collectorid:timer 中获取之前使用的定时器 TimerRef
    case gen_server:call(RedisTask, {get_heartbeat_timer_by_collectorid, CollectorID}, infinity) of
        {error, CollectorHBErr1} ->
            lager:warning("[nms_task_control] 'HGET collector:~p:timer heartbeat' -- Failed! Error '~p'~n", 
                [CollectorID, CollectorHBErr1]),
            OldTimerRef_ = undefined;
        {ok, undefined} ->
            lager:info("[nms_task_control] 'HGET collector:~p:timer heartbeat' -- Success! HBTimerRef=undefined~n", 
                [CollectorID]),
            OldTimerRef_ = undefined;
        {ok, OldTimerRef_} ->
            lager:info("[nms_task_control] 'HGET collector:~p:timer heartbeat' -- Success! HBTimerRef=~p~n", 
                [CollectorID, binary_to_term(OldTimerRef_)])
    end,

    case OldTimerRef_ of
        undefined ->
            void;
        _ ->
            OldTimerRef = binary_to_term(OldTimerRef_),
            %% 删除之前的定时器
            case cancel_timer(OldTimerRef) of
                false ->  %% 对应定时器已被删除的情况
                    lager:notice("[nms_task_control] Trying to cancel a non-exist timer! ~p~n", [OldTimerRef]);
                0     ->  %% 对应定时器已超时情况
                    lager:notice("[nms_task_control] Timer is ALREADY Triggered! ~p~n", [OldTimerRef]);
                    %% 从 redis 表中删除 collector 相关信息
                _     ->  %% 对应定时器未超时情况
                    lager:notice("[nms_task_control] Timer is STILL Active! ~p~n", [OldTimerRef])                            
            end
    end,

    %% 设置新的定时器
    NewTimerRef = erlang:start_timer(?COLLECTOR_TIMEOUT, self(), {delete_collector_info, CollectorID}),
    NewTimerRefList = binary_to_list(term_to_binary(NewTimerRef)), 

    %% 更新 collectorid 与 TimerRef 的映射
    %% 根据 collectorid 向 Redis 表 collector:collectorid:timer 中保存新的定时器 TimerRef
    case gen_server:call(RedisTask, {set_heartbeat_timer_by_collectorid, CollectorID, NewTimerRefList}, infinity) of
        {error, CollectorHBErr2} ->
            lager:warning("[nms_task_control] 'HSET collector:~p:timer heartbeat' -- Failed! Error '~p'~n", 
                [CollectorID, CollectorHBErr2]);
        {ok, _} ->
            lager:info("[nms_task_control] 'HSET collector:~p:timer heartbeat ~p' -- Success! ~n", 
                [CollectorID, NewTimerRef])
    end.



physical_device_proc(JsonObj, RedisTask, MySQLTask) ->

    DevGuid_ = rfc4627:get_field(JsonObj, "devid", undefined),
    lager:info("  -->  DevGuid = ~p~n", [DevGuid_]),
    DevGuid = binary_to_list(DevGuid_),

    %% rpttime 格式     year-month-day/hour:min:sec
    %% rpttime 格式变更 year/month/day:hour:min:sec
    StatisticTime_ = rfc4627:get_field(JsonObj, "rpttime", undefined),
    case StatisticTime_ of
        undefined ->
            {{Year,Month,Day},{Hour,Min,Sec}} = calendar:now_to_local_time(os:timestamp()),
            StatisticTime = integer_to_list(Year)++"/"++integer_to_list(Month)++"/"++integer_to_list(Day)++":"++
                            integer_to_list(Hour)++":"++integer_to_list(Min)++":"++integer_to_list(Sec),
            lager:info("[nms_task_control] find no rpttime, so make it myself:~p~n", 
                [StatisticTime]);
        _ ->
            StatisticTime = binary_to_list(StatisticTime_)
    end,
    lager:info("  -->  StatisticTime = ~p~n", [StatisticTime]),

    EventID = rfc4627:get_field(JsonObj, "eventid", undefined),
    lager:info("  -->  EventID = ~p~n", [EventID]),

    %% 通过 "devid" 在 redis 表 p_server:devid:info 中查询当前物理设备相关信息
    case gen_server:call(RedisTask, {get_physical_server_info_by_guid, DevGuid_}, infinity) of
        { ok, {DevMoid_, DevGuid_, DomainMoid_, DevName, Location, IP} } ->
            lager:info("  -->  DevMoid = ~p~n", [DevMoid_]),
            lager:info("  -->  DomainMoid = ~p~n", [DomainMoid_]),
            lager:info("  -->  DevName = ~p~n", [DevName]),
            lager:info("  -->  Location = ~ts~n", [Location]),
            lager:info("  -->  IP = ~p~n", [IP]),

            DevMoid = binary_to_list(DevMoid_),
            DomainMoid = binary_to_list(DomainMoid_),

            DevType_ = rfc4627:get_field(JsonObj, "devtype", undefined),
            DevType = binary_to_list(DevType_),

            %% 通过 "eventid" 判定为信息类型
            case EventID of
                <<"EV_SERVER_INFO">>   ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"111">>},
                    %%       {"devtype",<<"Server">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"serverinfo",
                    %%           [{obj,[{"ip",<<"172.16.100.1">>}]},
                    %%            {obj,[{"ip",<<"172.16.110.1">>}]}]},
                    %%       {"eventid",<<"EV_SERVER_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_SERVER_INFO' event!~n", []),

                    ServerInfo = rfc4627:get_field(JsonObj, "serverinfo", undefined),

                    PhySerIPList = [ erlang:binary_to_list(NetCardIP) || {obj, [{_, NetCardIP}]} <- ServerInfo],
                    PhySerIPString = lists:concat(lists:map(fun(E) -> E ++ ";" end, PhySerIPList)),

                    lager:info("  -->  IPs = ~p~n", [PhySerIPString]),

                    %% 向 redis 表 p_server:devid:info 中插入上面得到的 IP 地址字符串
                    case gen_server:call(RedisTask, {add_physical_ip, DevMoid, PhySerIPString}, infinity) of
                        {error, PhySrvInfoErr0} ->
                            lager:warning("[nms_task_control] 'HSET p_server:~p:info ip xx' -- Failed! Error '~p'~n", 
                                [DevMoid, PhySrvInfoErr0]);
                        _ ->
                            lager:info("[nms_task_control] 'HSET p_server:~p:info ip ~p' -- Success!~n", 
                                [DevMoid, PhySerIPString])
                    end,
                    io:format("", []);

                <<"EV_SYSTIME_SYNC">>   ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"111">>},
                    %%       {"devtype",<<"Server">>},
                    %%       {"rpttime",<<"2014/11/20:20:13:55">>},
                    %%       {"eventid",<<"EV_SYSTIME_SYNC">>},
                    %%       {"syncstate",1}]}

                    lager:info("[nms_task_control] get 'EV_SYSTIME_SYNC' event!~n", []),

                    Sync_ = rfc4627:get_field(JsonObj, "syncstate", undefined),
                    lager:info("  -->  Sync = ~p~n", [Sync_]),

                    PhySyncWarningTriggered = case Sync_ of
                        0 -> %% NTP异常
                            true;
                        1 ->
                            false;
                        _ ->
                            throw(physical_ntp_sync_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PhySyncWarningTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2004), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PhySyncWarningTriggered,RedisTask,DevMoid,?P_SERVER,2004), 

        %% ------------------------------------------------------------
                    io:format("", []);

                <<"EV_DEV_ONLINE">>     ->
                    %% 消息举例
                    %% {obj,[{"eventid",<<"EV_DEV_ONLINE">>},
                    %%       {"devid",<<"111">>},
                    %%       {"devtype",<<"SERVICE_SRV_PHY">>},
                    %%       {"collectorid",<<"60a44c502a60">>}]}

                    lager:info("[nms_task_control] get 'EV_DEV_ONLINE' event!~n", []),

                    CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
                    lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
                    CollectorID = binary_to_list(CollectorID_),

                    %%  设置 Redis 表 p_server:devid:online 的值为 online
                    case gen_server:call(RedisTask, {add_physical_server_online, DevMoid}, infinity) of
                        {error, PhyDevOnlineErr0} ->
                            lager:warning("[nms_task_control] 'SET p_server:~p:online online' -- Failed! Error '~p'~n", 
                                [DevMoid, PhyDevOnlineErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SET p_server:~p:online online' -- Success!~n", [DevMoid])
                    end,

                    %% 向 Redis 表 collector 中保存 collectorid
                    case gen_server:call(RedisTask, {add_collectorid, CollectorID}, infinity) of
                        {error, PhyDevOnlineErr1} ->
                            lager:warning("[nms_task_control] 'SADD collector ~p' -- Failed! Error '~p'~n", 
                                [CollectorID, PhyDevOnlineErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD collector ~p' -- Success!~n", [CollectorID])
                    end,

                    %% 向 redis 表 collector:collectorid:online 中写入 devtype:devid 信息
                    case gen_server:call(RedisTask, {add_collector_online_device, CollectorID, DevGuid, DevType}, infinity) of
                        {error, PhyDevOnlineErr2} ->
                            lager:warning("[nms_task_control] 'SADD collector:~p:online' -- Failed! Error '~p'~n", 
                                [CollectorID, PhyDevOnlineErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD collector:~p:online ~p:~p' -- Success!~n", 
                                [CollectorID, DevType, DevGuid])
                    end,
                    io:format("", []);

                <<"EV_DEV_OFFLINE">>     ->
                    %% 消息举例
                    %% {obj,[{"eventid",<<"EV_DEV_OFFLINE">>},
                    %%       {"devid",<<"111">>},
                    %%       {"devtype",<<"SERVICE_SRV_PHY">>},
                    %%       {"collectorid",<<"60a44c502a60">>}]}

                    lager:info("[nms_task_control] get 'EV_DEV_OFFLINE' event!~n", []),

                    CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
                    lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
                    CollectorID = binary_to_list(CollectorID_),

                    %%  删除 Redis 表 p_server:devid:online
                    case gen_server:call(RedisTask, {del_physical_server_online, DevMoid}, infinity) of
                        {error, PhyDevOfflineErr0} ->
                            lager:warning("[nms_task_control] 'DEL p_server:~p:online' -- Failed! Error '~p'~n", 
                                [DevMoid, PhyDevOfflineErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL p_server:~p:online' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 p_server:devid:resource
                    case gen_server:call(RedisTask, {del_physical_server_resource, DevMoid}, infinity) of
                        {error, PhyDevOfflineErr1} ->
                            lager:warning("[nms_task_control] 'DEL p_server:~p:resource' -- Failed! Error '~p'~n", 
                                [DevMoid, PhyDevOfflineErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL p_server:~p:resource' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 p_server:devid:warning
                    case gen_server:call(RedisTask, {del_physical_server_warning_all, DevMoid}, infinity) of
                        {error, PhyDevOfflineErr2} ->
                            lager:warning("[nms_task_control] 'DEL p_server:~p:warning' -- Failed! Error '~p'~n", 
                                [DevMoid, PhyDevOfflineErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL p_server:~p:warning' -- Success!~n", [DevMoid])
                    end,

                    %% 从 Redis 表 collector:collectorid:online 中删除 devtype:devid 信息
                    case gen_server:call(RedisTask, {del_collector_online_device, CollectorID, DevGuid, DevType}, infinity) of
                        {error, PhyDevOnlineErr3} ->
                            lager:warning("[nms_task_control] 'SREM collector:~p:online' -- Failed! Error '~p'~n", 
                                [CollectorID, PhyDevOnlineErr3]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SREM collector:~p:online ~p:~p' -- Success!~n", 
                                [CollectorID, DevType, DevGuid])
                    end,
                    io:format("", []);

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
                    CoreNum   = rfc4627:get_field(CPUInfo, "cpucorecount", 0),
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


                    %% 查询保存服务器 CPU 阈值信息的表 resource_limit
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
                    update_mysql_warning_info(CpuWarningTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2002), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(CpuWarningTriggered,RedisTask,DevMoid,?P_SERVER,2002), 

        %% ------------------------------------------------------------

                    %% 向 redis 表 p_server:devid:resource 保存当前多核 CPU 的平均使用率
                    CpuAverage = lists:sum(CpuPctList) div CoreNum,     
                    case gen_server:call(RedisTask, 
                            {update_physical_server_cpu_resource, binary_to_list(DevMoid_), CpuAverage}, infinity) of
                        {error, CpuErr4} ->
                            lager:warning("[nms_task_control] 'HMSET p_server:~p:resource cpu xx' -- Failed! Error '~p'~n", 
                                [DevMoid, CpuErr4]);
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
                    update_mysql_warning_info(MemWarningTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2003), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(MemWarningTriggered,RedisTask,DevMoid,?P_SERVER,2003), 

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

                    case gen_server:call(MySQLTask, {add_disk_statistic,DomainMoid,DevMoid,DiskUsePct,StatisticTime}, infinity) of
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
                    update_mysql_warning_info(DiskWarningTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2018), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(DiskWarningTriggered,RedisTask,DevMoid,?P_SERVER,2018), 

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
                    %%           {obj,[{"netcardcount",2},
                    %%                 {"recvpktloserate",0},
                    %%                 {"recvkbps",0},
                    %%                 {"sendkbps",0},
                    %%                 {"netcards",
                    %%                     [{obj,[{"netcard1",
                    %%                                {obj,[{"recvpktloserate",0},
                    %%                                      {"sendkbps",0},
                    %%                                      {"recvkbps",0}]}}]},
                    %%                      {obj,[{"netcard2",
                    %%                                {obj,[{"recvpktloserate",0},
                    %%                                      {"sendkbps",0},
                    %%                                      {"recvkbps",0}]}}]}]}]}}]}

                    NetCardInfo   = rfc4627:get_field(JsonObj, "netcardinfo", undefined),
                    NetCardNum = rfc4627:get_field(NetCardInfo, "netcardcount", 0),
                    RecvLostPctTotal  = rfc4627:get_field(NetCardInfo, "recvpktloserate", 0),
                    RecvKBytesTotal  = rfc4627:get_field(NetCardInfo, "recvkbps", 0),
                    SendKBytesTotal  = rfc4627:get_field(NetCardInfo, "sendkbps", 0),
                    Netcards  = rfc4627:get_field(NetCardInfo, "netcards", 0),

                    lager:info("  -->  netcardcount = ~p~n", [NetCardNum]),
                    lager:info("  -->  total recvpktloserate = ~p~n", [RecvLostPctTotal]),
                    lager:info("  -->  total recvkbps = ~p~n", [RecvKBytesTotal]),
                    lager:info("  -->  total sendkbps = ~p~n", [SendKBytesTotal]),

                    io:format("~n"),

                    %% 将每个 Netcard 上的发送/接收流量写入 netcard_statistic 统计表
                    [begin
                        RecvMBytes = RecvKBytes / 1024,
                        SendMBytes = SendKBytes / 1024,
                        RecvMBytesList = erlang:float_to_list(RecvMBytes, [{decimals, 2}]),
                        SendMBytesList = erlang:float_to_list(SendMBytes, [{decimals, 2}]),
                        gen_server:call(MySQLTask, {add_netcard_statistic, DomainMoid, DevMoid, list_to_integer(Index), 
                                RecvMBytesList, SendMBytesList, StatisticTime}, infinity)
                    end || {obj,[{"netcard"++Index,
                                     {obj,[{"recvpktloserate",_RecvLostPct},
                                           {"sendkbps",SendKBytes},
                                           {"recvkbps",RecvKBytes}]}}]} <- Netcards],                                           

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
                    NetTrafficList_ = [ [S,R] || {obj,[{_,{obj,[{_,_},{"sendkbps",S},{"recvkbps",R}]}}]} <- Netcards],
                    NetTrafficList = lists:flatten(NetTrafficList_),
                    Predicate = fun(V) -> 
                                    V >= list_to_integer(binary_to_list(Net_Threshold))
                                end,
                    NetTrafficWarningTriggered = lists:any(Predicate, NetTrafficList),

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(NetTrafficWarningTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2019), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(NetTrafficWarningTriggered,RedisTask,DevMoid,?P_SERVER,2019), 

        %% ------------------------------------------------------------

                    %% 保存 RecvBytesTotal 和 SendBytesTotal 值到 redis 表 p_server:devid:resource
                    case gen_server:call(RedisTask, {update_physical_server_net_resource, DevMoid, RecvKBytesTotal, SendKBytesTotal}, 
                            infinity) of
                        {error, DiskErr2} ->
                            lager:warning("[nms_task_control] 'HMSET p_server:~p:resource portin xx portout xx' -- Failed! Error '~p'~n", 
                                [DevMoid, DiskErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET p_server:~p:resource portin ~p portout ~p' -- Success!~n", 
                                [DevMoid, RecvKBytesTotal, SendKBytesTotal])
                    end,


                    %% 判定当前多 NetCard 网卡设备上单 NetCard 接收丢包率是否触发告警状态
                    PacketLostPctList = [ PacketLostPct || {obj,[{_,{obj,[{"recvpktloserate",PacketLostPct},_,_]}}]} <- Netcards],

                    %% 判定否超过 5% 和 10% -- 分别对应告警码 2013 和 2014
                    Fun10 = fun(V10) -> V10 >= 10 end,                    
                    Pct10 = lists:any(Fun10, PacketLostPctList),

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(Pct10,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2014),

        %% ------------------------------------------------------------

                    update_redis_warning_info(Pct10,RedisTask,DevMoid,?P_SERVER,2014),

        %% ------------------------------------------------------------

                    Fun5 = fun(V5) -> V5 >= 5 end,
                    Pct5 = lists:any(Fun5, PacketLostPctList),

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(Pct5,MySQLTask,DevMoid,DomainMoid,
                        ?P_SERVER,StatisticTime,EventID,2013),

        %% ------------------------------------------------------------

                    update_redis_warning_info(Pct5,RedisTask,DevMoid,?P_SERVER,2013), 

        %% ------------------------------------------------------------

                    io:format("", []);
                OtherEvent     ->
                    lager:info("[nms_task_control] get '~p' event, do nothing!~n", [OtherEvent])
            end;

        {error,<<"Key Error">>} ->
            lager:warning("[nms_task_control] get <<\"Key Error\">> for key ~p~n", [DevGuid]),
            throw(redis_key_error);
        {error, no_connection} ->
            lager:error("[nms_task_control] lost redis connection!"),
            throw(redis_connection_lost)
    end.


logical_device_proc(JsonObj, RedisTask, MySQLTask, CustomType) ->

    %% rpttime 格式 year-month-day/hour:min:sec
    StatisticTime_ = rfc4627:get_field(JsonObj, "rpttime", undefined),
    case StatisticTime_ of
        undefined ->
            {{Year,Month,Day},{Hour,Min,Sec}} = calendar:now_to_local_time(os:timestamp()),
            StatisticTime = integer_to_list(Year)++"/"++integer_to_list(Month)++"/"++integer_to_list(Day)++":"++
                            integer_to_list(Hour)++":"++integer_to_list(Min)++":"++integer_to_list(Sec),
            lager:info("[nms_task_control] find no rpttime, so make it myself:~p~n", 
                [StatisticTime]);
        _ ->
            StatisticTime = binary_to_list(StatisticTime_)
    end,
    lager:info("  -->  StatisticTime = ~p~n", [StatisticTime]),

    EventID = rfc4627:get_field(JsonObj, "eventid", undefined),
    lager:info("  -->  EventID = ~p~n", [EventID]),

    DevGuid_ = rfc4627:get_field(JsonObj, "devid", undefined),
    lager:info("  -->  Logical Device Guid = ~p~n", [DevGuid_]),
    DevGuid = binary_to_list(DevGuid_),

    %% 通过 "devid" 在 redis 中查询当前逻辑设备所属的域 ID
    case gen_server:call(RedisTask, {get_logic_server_info_by_guid, DevGuid_}, infinity) of
        { ok, {DevMoid_, DevGuid_, PlatformDomainMoid_, DomainMoid_, DevName, IP, Type} } ->
            lager:info("  -->  Logical Device Moid = ~p~n", [DevMoid_]),
            lager:info("  -->  Platform Domain Moid = ~p~n", [PlatformDomainMoid_]),
            lager:info("  -->  Service Domain Moid = ~p~n", [DomainMoid_]),
            lager:info("  -->  Logical Device Name = ~p~n", [DevName]),
            lager:info("  -->  Logical Device Type (Redis) = ~p~n", [Type]),
            lager:info("  -->  IP = ~p~n", [IP]),

            DevMoid = binary_to_list(DevMoid_),
            PlatformDomainMoid = binary_to_list(PlatformDomainMoid_),
            DomainMoid = binary_to_list(DomainMoid_),

            DevType_ = rfc4627:get_field(JsonObj, "devtype", undefined),
            DevType = binary_to_list(DevType_),

            %% 通过 "eventid" 判定为信息类型
            case EventID of

                <<"EV_DEV_ONLINE">>     ->
                    %% 消息举例
                    %% {obj,[{"eventid",<<"EV_DEV_ONLINE">>},
                    %%       {"devid",<<"1111">>},
                    %%       {"devtype",<<"SERVICE_TS_SRV_MPCD">>},
                    %%       {"collectorid",<<"60a44c502a60">>}]}

                    lager:info("[nms_task_control] get 'EV_DEV_ONLINE' event!~n", []),

                    CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
                    lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
                    CollectorID = binary_to_list(CollectorID_),

                    %%  设置 Redis 表 l_server:devid:online 的值为 online
                    case gen_server:call(RedisTask, {add_logic_server_online, DevMoid}, infinity) of
                        {error, LogDevOnlineErr0} ->
                            lager:warning("[nms_task_control] 'SET l_server:~p:online online' -- Failed! Error '~p'~n", 
                                [DevMoid, LogDevOnlineErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SET l_server:~p:online online' -- Success!~n", [DevMoid])
                    end,

                    %% 向 Redis 的 SET 表 collector 中保存 collectorid
                    case gen_server:call(RedisTask, {add_collectorid, CollectorID}, infinity) of
                        {error, LogDevOnlineErr1} ->
                            lager:warning("[nms_task_control] 'SADD collector ~p' -- Failed! Error '~p'~n", 
                                [CollectorID, LogDevOnlineErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD collector ~p' -- Success!~n", [CollectorID])
                    end,

                    %% 向 redis 表 collector:collectorid:online 中写入 devtype:devid 信息
                    case gen_server:call(RedisTask, {add_collector_online_device, CollectorID, DevGuid, DevType}, infinity) of
                        {error, LogDevOnlineErr2} ->
                            lager:warning("[nms_task_control] 'SADD collector:~p:online' -- Failed! Error '~p'~n", 
                                [CollectorID, LogDevOnlineErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD collector:~p:online ~p:~p' -- Success!~n", 
                                [CollectorID, DevType, DevGuid])
                    end,

                    io:format("", []);

                <<"EV_DEV_OFFLINE">>     ->
                    %% 消息举例
                    %% {obj,[{"eventid",<<"EV_DEV_OFFLINE">>},
                    %%       {"devid",<<"1111">>},
                    %%       {"devtype",<<"SERVICE_TS_SRV_MPCD">>},
                    %%       {"collectorid",<<"60a44c502a60">>}]}

                    lager:info("[nms_task_control] get 'EV_DEV_OFFLINE' event!~n", []),

                    CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
                    lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
                    CollectorID = binary_to_list(CollectorID_),

                    %%  删除 Redis 表 l_server:devid:online
                    case gen_server:call(RedisTask, {del_logic_server_online, DevMoid}, infinity) of
                        {error, LogDevOfflineErr0} ->
                            lager:warning("[nms_task_control] 'DEL l_server:~p:online' -- Failed! Error '~p'~n", 
                                [DevMoid, LogDevOfflineErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL l_server:~p:online' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 l_server:devid:connection
                    case gen_server:call(RedisTask, {del_logic_server_connections, DevMoid}, infinity) of
                        {error, LogDevOfflineErr1} ->
                            lager:warning("[nms_task_control] 'DEL l_server:~p:connection' -- Failed! Error '~p'~n", 
                                [DevMoid, LogDevOfflineErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL l_server:~p:connection' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 l_server:devid:warning
                    case gen_server:call(RedisTask, {del_logic_server_warning_all, DevMoid}, infinity) of
                        {error, LogDevOfflineErr2} ->
                            lager:warning("[nms_task_control] 'DEL l_server:~p:warning' -- Failed! Error '~p'~n", 
                                [DevMoid, LogDevOfflineErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL l_server:~p:warning' -- Success!~n", [DevMoid])
                    end,

                    %% 从 Redis 表 collector:collectorid:online 中删除 devtype:devid 信息
                    case gen_server:call(RedisTask, {del_collector_online_device, CollectorID, DevGuid, DevType}, infinity) of
                        {error, LogDevOfflineErr3} ->
                            lager:warning("[nms_task_control] 'SREM collector:~p:online' -- Failed! Error '~p'~n", 
                                [CollectorID, LogDevOfflineErr3]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SREM collector:~p:online ~p:~p' -- Success!~n", 
                                [CollectorID, DevType, DevGuid])
                    end,

                    case CustomType of
                        'PAS' ->
                            %% 从 redis 的 SET 表 pas_in_all_domains 中删除 domain:PlatformDomainMoid:pas:pas_moid 字符串
                            CompletePasDomainInfo = "domain:" ++ PlatformDomainMoid ++ ":pas:" ++ DevMoid,
                            case gen_server:call(RedisTask, {del_pas_in_all_domains, CompletePasDomainInfo}, infinity) of
                                {error, LogDevOfflineErr4} ->
                                    lager:warning("[nms_task_control] 'SREM pas_in_all_domains ~p' -- Failed! Error '~p'~n", 
                                        [CompletePasDomainInfo, LogDevOfflineErr4]);
                                {ok, _} ->
                                    lager:info("[nms_task_control] 'SREM pas_in_all_domains ~p' -- Success!~n", 
                                        [CompletePasDomainInfo])
                            end;
                        'XMPP' ->
                            %% 从 redis 的 SET 表 xmpp_in_all_domains 中删除 domain:DomainMoid:xmpp_online 字符串
                            XmppDomainKey = "domain:" ++ DomainMoid ++ ":xmpp_online",
                            case gen_server:call(RedisTask, {del_xmpp_in_all_domains, XmppDomainKey}, infinity) of
                                {error, LogDevOfflineErr5} ->
                                    lager:warning("[nms_task_control] 'SREM xmpp_in_all_domains ~p' -- Failed! Error '~p'~n", 
                                        [XmppDomainKey, LogDevOfflineErr5]);
                                {ok, _} ->
                                    lager:info("[nms_task_control] 'SREM xmpp_in_all_domains ~p' -- Success!~n", 
                                        [XmppDomainKey])
                            end;
                        _ ->
                            void
                    end,

                    io:format("", []);

                <<"EV_XMPP_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"XMPP">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"xmppinfo",
                    %%           {obj,[{"onlinecount",123}]}},
                    %%       {"eventid",<<"EV_XMPP_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_XMPP_INFO' event!~n", []),

                    XmppVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  XMPP Version = ~p~n", [XmppVersion_]),

                    XmppInfo = rfc4627:get_field(JsonObj, "xmppinfo", undefined),
                    OnlineNum = rfc4627:get_field(XmppInfo, "onlinecount", undefined),
                    lager:info("  -->  XMPP Online = ~p~n", [OnlineNum]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  XMPP PidChanged = ~p~n", [PidChange_]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                    %% 向 redis 的 STRING 表 domain:DomainMoid:xmpp_online 中保存 XMPP 在线数
                    case gen_server:call(RedisTask, {add_xmpp_online_statistic, DomainMoid, OnlineNum}, infinity) of
                        {error, XmppInfoErr0} ->
                            lager:warning("[nms_task_control] 'SET domain:~p:xmpp_online ~p' -- Failed! Error '~p'~n", 
                                [DomainMoid, OnlineNum, XmppInfoErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SET domain:~p:xmpp_online ~p' -- Success!~n", 
                                [DomainMoid, OnlineNum])
                    end,

                    %% 向 redis 的 SET 表 xmpp_in_all_domains 中保存 domain:DomainMoid:xmpp_online 字符串
                    XmppDomainKey = "domain:" ++ DomainMoid ++ ":xmpp_online",
                    case gen_server:call(RedisTask, {add_xmpp_in_all_domains, XmppDomainKey}, infinity) of
                        {error, XmppInfoErr1} ->
                            lager:warning("[nms_task_control] 'SADD xmpp_in_all_domains ~p' -- Failed! Error '~p'~n", 
                                [XmppDomainKey, XmppInfoErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD xmpp_in_all_domains ~p' -- Success!~n", [XmppDomainKey])
                    end,

                    io:format("", []);

                <<"EV_NDS_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"NDS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"ndsinfo",
                    %%           {obj,[{"maxtestcount",123},
                    %%                 {"currtestcount",123}]}},
                    %%       {"eventid",<<"EV_NDS_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_NDS_INFO' event!~n", []),

                    NdsVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  NDS Version = ~p~n", [NdsVersion_]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  NDS PidChanged = ~p~n", [PidChange_]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                     %% 针对 ndsinfo 的处理

                    io:format("", []);

                <<"EV_LGS_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"LGS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"lgsinfo",
                    %%           {obj,[{"maxsrvcount",123},
                    %%                 {"logcount",123},
                    %%                 {"currsrvcount",123}]}},
                    %%       {"eventid",<<"EV_LGS_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_LGS_INFO' event!~n", []),

                    LgsVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  LGS Version = ~p~n", [LgsVersion_]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  LGS PidChanged = ~p~n", [PidChange_]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                     %% 针对 lgsinfo 的处理

                    io:format("", []);

                <<"EV_APS_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"APS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"apsinfo",
                    %%           {obj,[{"maxusercount",123},
                    %%                 {"currusercount",123}]}},
                    %%       {"eventid",<<"EV_APS_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_APS_INFO' event!~n", []),

                    ApsVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  APS Version = ~p~n", [ApsVersion_]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  APS PidChanged = ~p~n", [PidChange_]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                     %% 针对 apsinfo 的处理

                    io:format("", []);

                <<"EV_SUSMGR_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"SUSMGR">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"susmgrinfo",
                    %%           {obj,[{"maxregsussrvcount",123},
                    %%                 {"curregsussrvcount",123}]}},
                    %%       {"eventid",<<"EV_SUSMGR_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_SUSMGR_INFO' event!~n", []),

                    SusMgrVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  SUS Manager Version = ~p~n", [SusMgrVersion_]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  SUS PidChanged = ~p~n", [PidChange_]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                     %% 针对 susmgrinfo 的处理

                    io:format("", []);

                <<"EV_SUS_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"SUS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"susinfo",
                    %%           {obj,[{"maxregdevcount",123},
                    %%                 {"curregdevcount",123}]}},
                    %%       {"eventid",<<"EV_SUS_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_SUS_INFO' event!~n", []),

                    SusVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  SUS Version = ~p~n", [SusVersion_]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  SUS PidChanged = ~p~n", [PidChange_]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                     %% 针对 susinfo 的处理

                    io:format("", []);

                <<"EV_MCU_MT_DEL">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"CMS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"mtinfo",
                    %%           {obj,[{"endtime",<<"2014/06/16:09:57:50">>},
                    %%                 {"mte164",<<"0512111885808">>},
                    %%                 {"confe164",<<"0513**77">>},
                    %%                 {"leavereason",<<"down">>},
                    %%                 {"conftype",0}]}},
                    %%       {"eventid",<<"EV_MCU_MT_DEL">>}]}

                    lager:info("[nms_task_control] get 'EV_MCU_MT_DEL' event!~n", []),

                    MtInfo = rfc4627:get_field(JsonObj, "mtinfo", undefined),

                    TerminalE164_ = rfc4627:get_field(MtInfo, "mte164", undefined),
                    lager:info("  -->  Terminal E164 = ~p~n", [TerminalE164_]),
                    TerminalE164 = binary_to_list(TerminalE164_), 

                    ConfE164_ = rfc4627:get_field(MtInfo, "confe164", undefined),
                    lager:info("  -->  Conf E164 = ~p~n", [ConfE164_]),
                    ConfE164 = binary_to_list(ConfE164_), 

                    TerLeaveTime_ = rfc4627:get_field(MtInfo, "endtime", undefined),
                    lager:info("  -->  Terminal Leave Meeting Time = ~p~n", [TerLeaveTime_]),
                    TerLeaveTime = binary_to_list(TerLeaveTime_), 

                    TerLeaveReason_ = rfc4627:get_field(MtInfo, "leavereason", undefined),
                    lager:info("  -->  Terminal Leave Meeting Reason = ~p~n", [TerLeaveReason_]),
                    TerLeaveReason = binary_to_list(TerLeaveReason_), 

                    ConfType = rfc4627:get_field(MtInfo, "conftype", undefined),
                    case ConfType of
                        0 ->
                            lager:info("  -->  Conf Type = Traditional~n", []),

                            %% 将 redis 的 HASH 表 t_meeting:ConfE164:info 中的 terminal 的值 -1
                            case gen_server:call(RedisTask, {dec_traditional_meeting_terminal_num, ConfE164}, infinity) of
                                {error, McuMtDelErr0} ->
                                    lager:warning("[nms_task_control] 'HINCRBY t_meeting:~p:info terminal -1' -- Failed! Error '~p'~n", 
                                        [ConfE164, McuMtDelErr0]);

                                {ok, TMeetingTerNum} ->
                                    lager:info("[nms_task_control] 'HINCRBY t_meeting:~p:info terminal -1' -- Success! TMeetingTerNum=~p~n",
                                        [ConfE164, TMeetingTerNum])
                            end;
                        1 ->
                            lager:info("  -->  Conf Type = Port~n", []),

                            %% 将 redis 的 HASH 表 p_meeting:ConfE164:info 中的 terminal 的值 -1
                            case gen_server:call(RedisTask, {dec_port_meeting_terminal_num, ConfE164}, infinity) of
                                {error, McuMtDelErr1} ->
                                    lager:warning("[nms_task_control] 'HINCRBY p_meeting:~p:info terminal -1' -- Failed! Error '~p'~n",
                                        [ConfE164, McuMtDelErr1]);

                                {ok, PMeetingTerNum} ->
                                    lager:info("[nms_task_control] 'HINCRBY p_meeting:~p:info terminal -1' -- Success! PMeetingTerNum=~p~n",
                                        [ConfE164, PMeetingTerNum])
                            end;
                        _ ->
                            throw(conftype_enum_error)
                    end,

                    %% 从 redis 的 HASH 表 terminal:TerminalE164:baseinfo 查 TerminalMoid
                    case gen_server:call(RedisTask, {get_terminal_base_info_by_e164, TerminalE164}, infinity) of
                        {error, McuMtDelErr2} ->
                            lager:warning("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Failed! Error '~p'~n", 
                                [TerminalE164, McuMtDelErr2]),
                            lager:warning("[nms_task_control] Find no terminal info by E164, Make sure E164(~p) is Correct!~n",
                                [TerminalE164]),
                            TerminalMoid_ = <<"match no domain">>;
                        {ok, {TerminalMoid_,_,_,_}} ->
                            lager:info("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Success! TerminalMoid=~p~n", 
                                [TerminalE164, TerminalMoid_])
                    end,
                    TerminalMoid = binary_to_list(TerminalMoid_),

                    %% 将 redis 的 STRING 表 terminal:TerminalMoid:conf:ConfE164:enter_times 中的值 +0（获取当前值）
                    case gen_server:call(RedisTask, {get_terminal_enter_meeting_times, TerminalMoid, ConfE164}, infinity) of
                        {error, McuMtDelErr3} ->
                            lager:warning("[nms_task_control] 'INCRBY terminal:~p:conf:~p:enter_times +0' -- Failed! Error '~p'~n",
                                [TerminalMoid, ConfE164, McuMtDelErr3]);

                        {ok, TerEnterTimes_} ->
                            lager:info("[nms_task_control] 'INCRBY terminal:~p:conf:~p:enter_times +0' -- Success! TerEnterTimes=~p~n", 
                                [TerminalMoid, ConfE164, TerEnterTimes_]),

                            TerEnterTimes = binary_to_list(TerEnterTimes_), 

                            %% 向 redis 的 HASH 表 terminal:TerminalMoid:conf:ConfE164:enter_leave_info:N 中
                            %% 保存 leave_time 和 leave_reason
                            case gen_server:call(RedisTask, {add_terminal_leave_meeting_info, TerminalMoid, ConfE164, 
                                    TerEnterTimes, TerLeaveTime, TerLeaveReason}, infinity) of
                                {error, McuMtDelErr4} ->
                                    lager:warning("[nms_task_control] 'HMSET terminal:~p:conf:~p:enter_leave_info:~p' -- Failed! Error '~p'~n",
                                        [TerminalMoid, ConfE164, TerEnterTimes, McuMtDelErr4]);

                                {ok, _} ->
                                    lager:info("[nms_task_control] 'HMSET terminal:~p:conf:~p:enter_leave_info:~p' -- Success!~n", 
                                        [TerminalMoid, ConfE164, TerEnterTimes])
                            end
                    end,

                    %% 从 redis 的 SET 表 meeting:ConfE164:terminal 中删除 TerminalE164
                    case gen_server:call(RedisTask, {del_meeting_terminal, ConfE164, TerminalE164}, infinity) of
                        {error, McuMtDelErr5} ->
                            lager:warning("[nms_task_control] 'SREM meeting:~p:terminal' -- Failed! Error '~p'~n", 
                                [ConfE164, McuMtDelErr5]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SREM meeting:~p:terminal' -- Success! TerminalE164=~p~n", 
                                [ConfE164, TerminalE164])
                    end,

                    %% 删除 redis 的 HASH 表 terminal:TerminalMoid:meetingdetail
                    case gen_server:call(RedisTask, {del_terminal_meeting_detail, TerminalMoid}, infinity) of
                        {error, McuMtDelErr6} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:meetingdetail' -- Failed! Error '~p'~n", 
                                [TerminalMoid, McuMtDelErr6]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:meetingdetail' -- Success!~n", 
                                [TerminalMoid])
                    end,

                    %% 将如下 redis 表中的会议音视频路数相关信息全部删除
                    %% terminal:TerminalMoid:meetingdetail:privideo_send_chan
                    %% terminal:TerminalMoid:meetingdetail:privideo_recv_chan
                    %% terminal:TerminalMoid:meetingdetail:assvideo_send_chan
                    %% terminal:TerminalMoid:meetingdetail:assvideo_recv_chan
                    %% terminal:TerminalMoid:meetingdetail:audio_send_chan
                    %% terminal:TerminalMoid:meetingdetail:audio_recv_chan
                    case gen_server:call(RedisTask, {del_terminal_meeting_channels, TerminalMoid}, infinity) of
                        {error, McuMtDelErr7} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:meetingdetail:[chantpye]:[index]' -- Failed! Error '~p'~n", 
                                [TerminalMoid, McuMtDelErr7]),
                            lager:warning("[nms_task_control] 'DEL terminal:~p:meetingdetail:[chantpye]' -- Failed! Error '~p'~n", 
                                [TerminalMoid, McuMtDelErr7]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:meetingdetail:[chantpye]:[index]' -- Success!~n", 
                                [TerminalMoid]),
                            lager:info("[nms_task_control] 'DEL terminal:~p:meetingdetail:[chantpye]' -- Success!~n", 
                                [TerminalMoid])
                    end,

                    io:format("", []);

                <<"EV_MCU_MT_ADD">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"CMS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"mtinfo",
                    %%           {obj,[{"begintime",<<"2014/06/16:09:57:50">>},
                    %%                 {"mte164",<<"0512111885808">>},
                    %%                 {"confe164",<<"0513**77">>},
                    %%                 {"conftype",0}]}},
                    %%       {"eventid",<<"EV_MCU_MT_ADD">>}]}

                    lager:info("[nms_task_control] get 'EV_MCU_MT_ADD' event!~n", []),

                    MtInfo = rfc4627:get_field(JsonObj, "mtinfo", undefined),

                    TerminalE164_ = rfc4627:get_field(MtInfo, "mte164", undefined),
                    lager:info("  -->  Terminal E164 = ~p~n", [TerminalE164_]),
                    TerminalE164 = binary_to_list(TerminalE164_), 

                    ConfE164_ = rfc4627:get_field(MtInfo, "confe164", undefined),
                    lager:info("  -->  Conf E164 = ~p~n", [ConfE164_]),
                    ConfE164 = binary_to_list(ConfE164_), 

                    TerEnterTime_ = rfc4627:get_field(MtInfo, "begintime", undefined),
                    lager:info("  -->  Terminal Enter Meeting Time = ~p~n", [TerEnterTime_]),
                    TerEnterTime = binary_to_list(TerEnterTime_), 

                    ConfType = rfc4627:get_field(MtInfo, "conftype", undefined),
                    case ConfType of
                        0 ->
                            lager:info("  -->  Conf Type = Traditional~n", []),

                            %% 将 redis 的 HASH 表 t_meeting:ConfE164:info 中的 terminal 的值 +1
                            case gen_server:call(RedisTask, {inc_traditional_meeting_terminal_num, ConfE164}, infinity) of
                                {error, McuMtAddErr0} ->
                                    lager:warning("[nms_task_control] 'HINCRBY t_meeting:~p:info terminal +1' -- Failed! Error '~p'~n", 
                                        [ConfE164, McuMtAddErr0]);

                                {ok, TMeetingTerNum} ->
                                    lager:info("[nms_task_control] 'HINCRBY t_meeting:~p:info terminal +1' -- Success! TMeetingTerNum=~p~n",
                                        [ConfE164, TMeetingTerNum])
                            end;
                        1 ->
                            lager:info("  -->  Conf Type = Port~n", []),

                            %% 将 redis 的 HASH 表 p_meeting:ConfE164:info 中的 terminal 的值 +1
                            case gen_server:call(RedisTask, {inc_port_meeting_terminal_num, ConfE164}, infinity) of
                                {error, McuMtAddErr1} ->
                                    lager:warning("[nms_task_control] 'HINCRBY p_meeting:~p:info terminal +1' -- Failed! Error '~p'~n",
                                        [ConfE164, McuMtAddErr1]);

                                {ok, PMeetingTerNum} ->
                                    lager:info("[nms_task_control] 'HINCRBY p_meeting:~p:info terminal +1' -- Success! PMeetingTerNum=~p~n",
                                        [ConfE164, PMeetingTerNum])
                            end;
                        _ ->
                            throw(conftype_enum_error)
                    end,

                    %% 从 redis 的 HASH 表 terminal:TerminalE164:baseinfo 查 TerminalMoid
                    case gen_server:call(RedisTask, {get_terminal_base_info_by_e164, TerminalE164}, infinity) of
                        {error, McuMtAddErr2} ->
                            lager:warning("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Failed! Error '~p'~n", 
                                [TerminalE164, McuMtAddErr2]),
                            lager:warning("[nms_task_control] Find no terminal info by E164, Make sure E164(~p) is Correct!~n",
                                [TerminalE164]),
                            TerminalMoid_ = <<"match no domain">>;
                        {ok, {TerminalMoid_,_,_,_}} ->
                            lager:info("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Success! TerminalMoid=~p~n", 
                                [TerminalE164, TerminalMoid_])
                    end,
                    TerminalMoid = binary_to_list(TerminalMoid_),

                    %% 将 redis 的 STRING 表 terminal:TerminalMoid:conf:ConfE164:enter_times 中的值 +1
                    case gen_server:call(RedisTask, {inc_terminal_enter_meeting_times, TerminalMoid, ConfE164}, infinity) of
                        {error, McuMtAddErr3} ->
                            lager:warning("[nms_task_control] 'INCRBY terminal:~p:conf:~p:enter_times +1' -- Failed! Error '~p'~n",
                                [TerminalMoid, ConfE164, McuMtAddErr3]);

                        {ok, TerEnterTimes_} ->
                            lager:info("[nms_task_control] 'INCRBY terminal:~p:conf:~p:enter_times +1' -- Success! TerEnterTimes=~p~n", 
                                [TerminalMoid, ConfE164, TerEnterTimes_]),

                            TerEnterTimes = binary_to_list(TerEnterTimes_), 

                            %% 向 redis 的 HASH 表 terminal:TerminalMoid:conf:ConfE164:enter_leave_info:N 中保存 enter_time
                            case gen_server:call(RedisTask, 
                                    {add_terminal_enter_meeting_info, TerminalMoid, ConfE164, TerEnterTimes, TerEnterTime}, infinity) of
                                {error, McuMtAddErr4} ->
                                    lager:warning("[nms_task_control] 'HSET terminal:~p:conf:~p:enter_leave_info:~p' -- Failed! Error '~p'~n",
                                        [TerminalMoid, ConfE164, TerEnterTimes, McuMtAddErr4]);

                                {ok, _} ->
                                    lager:info("[nms_task_control] 'HSET terminal:~p:conf:~p:enter_leave_info:~p' -- Success!~n", 
                                        [TerminalMoid, ConfE164, TerEnterTimes])
                            end
                    end,

                    %% 向 redis 的 SET 表 meeting:ConfE164:terminal 中保存 TerminalE164
                    case gen_server:call(RedisTask, {add_meeting_terminal, ConfE164, TerminalE164}, infinity) of
                        {error, McuMtAddErr5} ->
                            lager:warning("[nms_task_control] 'SADD meeting:~p:terminal' -- Failed! Error '~p'~n", 
                                [ConfE164, McuMtAddErr5]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD meeting:~p:terminal' -- Success! TerminalE164=~p~n", 
                                [ConfE164, TerminalE164])
                    end,

                    io:format("", []);

                <<"EV_MCU_CONF_DESTROY">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"CMS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"confinfo",
                    %%           {obj,[{"endtime",<<"2014/06/16:10:57:50">>},
                    %%                 {"conftype",0},
                    %%                 {"confe164",<<"0512**999">>}]}},
                    %%       {"eventid",<<"EV_MCU_CONF_DESTROY">>}]}

                    lager:info("[nms_task_control] get 'EV_MCU_CONF_DESTROY' event!~n", []),

                    ConfInfo = rfc4627:get_field(JsonObj, "confinfo", undefined),

                    ConfE164_ = rfc4627:get_field(ConfInfo, "confe164", undefined),
                    lager:info("  -->  Conf E164 = ~p~n", [ConfE164_]),
                    ConfE164 = binary_to_list(ConfE164_), 

                    ConfType = rfc4627:get_field(ConfInfo, "conftype", undefined),
                    case ConfType of
                        0 ->
                            lager:info("  -->  Conf Type = Traditional~n", []),

                            case gen_server:call(RedisTask, {del_traditional_meeting, ConfE164}, infinity) of
                                {error, McuConfDestroyErr0} ->
                                    lager:warning("[nms_task_control] 'MULTI'~n", []),
                                    lager:warning("[nms_task_control] 'SREM domain:ConfDomainMoid:t_meeting ~p~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'DEL t_meeting:~p:info~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'DEL meeting:~p:terminal~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'EXEC' -- Failed! Error '~p'~n", [McuConfDestroyErr0]);

                                {ok, _} ->
                                    lager:info("[nms_task_control] 'MULTI'~n", []),
                                    lager:info("[nms_task_control] 'SREM domain:ConfDomainMoid:t_meeting ~p~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'DEL t_meeting:~p:info~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'DEL meeting:~p:terminal~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'EXEC' -- Success!~n", [])
                            end;
                        1 ->
                            lager:info("  -->  Conf Type = Port~n", []),

                            case gen_server:call(RedisTask, {del_port_meeting, ConfE164}, infinity) of
                                {error, McuConfDestroyErr1} ->
                                    lager:warning("[nms_task_control] 'MULTI'~n", []),
                                    lager:warning("[nms_task_control] 'SREM domain:ConfDomainMoid:p_meeting ~p~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'DEL p_meeting:~p:info~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'DEL meeting:~p:terminal~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'EXEC' -- Failed! Error '~p'~n", [McuConfDestroyErr1]);

                                {ok, _} ->
                                    lager:info("[nms_task_control] 'MULTI'~n", []),
                                    lager:info("[nms_task_control] 'SREM domain:ConfDomainMoid:p_meeting ~p~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'DEL p_meeting:~p:info~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'DEL meeting:~p:terminal~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'EXEC' -- Success!~n", [])
                            end;
                        _ ->
                            throw(conftype_enum_error)
                    end,

                    %% 

                    io:format("", []);

                <<"EV_MCU_CONF_CREATE">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"CMS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"confinfo",
                    %%           {obj,[{"confname",<<"conf-AA">>},
                    %%                 {"confe164",<<"0513**77">>},
                    %%                 {"conftype",0},
                    %%                 {"confbandwidth",123},
                    %%                 {"portcount",123},
                    %%                 {"domainmoid",<<"112">>},
                    %%                 {"domainname",<<"keda-AA">>},
                    %%                 {"begintime",<<"2014/06/16:09:57:50">>},
                    %%                 {"endtime",<<"2014/06/16:09:57:50">>}]}},
                    %%       {"eventid",<<"EV_MCU_CONF_CREATE">>}]}

                    lager:info("[nms_task_control] get 'EV_MCU_CONF_CREATE' event!~n", []),

                    ConfInfo = rfc4627:get_field(JsonObj, "confinfo", undefined),

                    ConfName_ = rfc4627:get_field(ConfInfo, "confname", undefined),
                    lager:info("  -->  Conf Name = ~p~n", [ConfName_]),
                    ConfName = binary_to_list(ConfName_),

                    ConfE164_ = rfc4627:get_field(ConfInfo, "confe164", undefined),
                    lager:info("  -->  Conf E164 = ~p~n", [ConfE164_]),
                    ConfE164 = binary_to_list(ConfE164_),

                    ConfBandWidth = rfc4627:get_field(ConfInfo, "confbandwidth", undefined),
                    lager:info("  -->  Conf BandWidth = ~p~n", [ConfBandWidth]),

                    ConfPortNum = rfc4627:get_field(ConfInfo, "portcount", undefined),
                    lager:info("  -->  Conf PortNum = ~p~n", [ConfPortNum]),

                    ConfDomainMoid_ = rfc4627:get_field(ConfInfo, "domainmoid", undefined),
                    lager:info("  -->  Conf DomainMoid = ~p~n", [ConfDomainMoid_]),
                    ConfDomainMoid = binary_to_list(ConfDomainMoid_),

                    ConfStartTime_ = rfc4627:get_field(ConfInfo, "begintime", undefined),
                    lager:info("  -->  Conf StartTime = ~p~n", [ConfStartTime_]),
                    ConfStartTime = binary_to_list(ConfStartTime_),

                    ConfStopTime_ = rfc4627:get_field(ConfInfo, "endtime", undefined),
                    lager:info("  -->  Conf StopTime = ~p~n", [ConfStopTime_]),
                    ConfStopTime = binary_to_list(ConfStopTime_),

                    ConfType = rfc4627:get_field(ConfInfo, "conftype", undefined),
                    case ConfType of
                        0 ->
                            lager:info("  -->  Conf Type = Traditional~n", []),

                            case gen_server:call(RedisTask, {add_traditional_meeting, ConfDomainMoid, ConfE164, ConfName, 
                                    ConfBandWidth, "mix", ConfStartTime, ConfStopTime}, infinity) of
                                {error, McuConfCreateErr0} ->
                                    lager:warning("[nms_task_control] 'MULTI'~n", []),
                                    lager:warning("[nms_task_control] 'SADD domain:~p:t_meeting ~p~n", [ConfDomainMoid, ConfE164]),
                                    lager:warning("[nms_task_control] 'HMSET t_meeting:~p:info~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'EXEC' -- Failed! Error '~p'~n", [McuConfCreateErr0]);

                                {ok, _} ->
                                    lager:info("[nms_task_control] 'MULTI'~n", []),
                                    lager:info("[nms_task_control] 'SADD domain:~p:t_meeting ~p~n", [ConfDomainMoid, ConfE164]),
                                    lager:info("[nms_task_control] 'HMSET t_meeting:~p:info~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'EXEC' -- Success!~n", [])
                            end;
                        1 ->
                            lager:info("  -->  Conf Type = Port~n", []),

                            case gen_server:call(RedisTask, {add_port_meeting, ConfDomainMoid, ConfE164, ConfName, ConfBandWidth, 
                                    ConfPortNum, ConfStartTime, ConfStopTime}, infinity) of
                                {error, McuConfCreateErr1} ->
                                    lager:warning("[nms_task_control] 'MULTI'~n", []),
                                    lager:warning("[nms_task_control] 'SADD domain:~p:p_meeting ~p~n", [ConfDomainMoid, ConfE164]),
                                    lager:warning("[nms_task_control] 'HMSET p_meeting:~p:info~n", [ConfE164]),
                                    lager:warning("[nms_task_control] 'EXEC' -- Failed! Error '~p'~n", [McuConfCreateErr1]);

                                {ok, _} ->
                                    lager:info("[nms_task_control] 'MULTI'~n", []),
                                    lager:info("[nms_task_control] 'SADD domain:~p:p_meeting ~p~n", [ConfDomainMoid, ConfE164]),
                                    lager:info("[nms_task_control] 'HMSET p_meeting:~p:info~n", [ConfE164]),
                                    lager:info("[nms_task_control] 'EXEC' -- Success!~n", [])
                            end;
                        _ ->
                            throw(conftype_enum_error)
                    end,

                    io:format("", []);

                <<"EV_MCU_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"CMS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"mcuinfo",
                    %%           {obj,[{"traditionconfcount",123},
                    %%                 {"portconfcount",123},
                    %%                 {"spttraditionconfcount",123},
                    %%                 {"sptportconfcount",123},
                    %%                 {"multiconfmtcount",123},
                    %%                 {"connectedprscount",123},
                    %%                 {"connectednucount",123},
                    %%                 {"connectedmpcount",123},
                    %%                 {"connectedmpcadaptcount",123}]}},
                    %%       {"eventid",<<"EV_MCU_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_MCU_INFO' event!~n", []),

                    %% 版本信息和升级服务器有关，暂时不做处理
                    MCUVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  MCU Version = ~p~n", [MCUVersion_]),
                    %%MCUVersion = binary_to_list(MCUVersion_),

                    %% 需要进行告警处理
                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  MCU PidChanged = ~p~n", [PidChange_]),

                    %% MCU 是否发生进程异常告警处理
                    PidChangeTriggered = case PidChange_ of
                        0 -> %% MCU 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mcu_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                     %% 针对 mcuinfo 的处理

                    io:format("", []);

                <<"EV_MPCD_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"MPCD">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"mpcdinfo",
                    %%           {obj,[{"maxorderconfcount",123},
                    %%                 {"connectednucount",123},
                    %%                 {"connectedmpccount",123},
                    %%                 {"conftempcount",123}]}},
                    %%       {"eventid",<<"EV_MPCD_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_MPCD_INFO' event!~n", []),

                    %% 版本信息和升级服务器有关，暂时不做处理
                    MPCDVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  MPCD Version = ~p~n", [MPCDVersion_]),
                    %%MPCDVersion = binary_to_list(MPCDVersion_),

                    %% 需要进行告警处理
                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  MPCD PidChanged = ~p~n", [PidChange_]),

                    %% MPCD 是否发生进程异常告警处理
                    PidChangeTriggered = case PidChange_ of
                        0 -> %% MPCD 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(mpcd_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                    %% 针对 mpcdinfo 的处理

                    io:format("", []);

                <<"EV_PAS_P2PCONF_CREATE">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"PAS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"confinfo",
                    %%           {obj,[{"caller",
                    %%                     {obj,[{"devtype",<<"Skywalker for iPad">>},
                    %%                           {"devname",<<"AAA">>},
                    %%                           {"deve164",<<"051255566">>}]}},
                    %%                 {"callee",
                    %%                     {obj,[{"devtype",<<"Skywalker for iPad">>},
                    %%                           {"devname",<<"BBB">>},
                    %%                           {"deve164",<<"051255567">>}]}},
                    %%                 {"time",<<"2014/06/16:09:57:50">>},
                    %%                 {"bitrate",256},
                    %%                 {"confe164",<<"1234">>},
                    %%                 {"confname",<<"conf-XXX">>}]}},
                    %%       {"eventid",<<"EV_PAS_P2PCONF_CREATE">>}]}

                    lager:info("[nms_task_control] get 'EV_PAS_P2PCONF_CREATE' event!~n", []),

                    P2PConfInfo = rfc4627:get_field(JsonObj, "confinfo", undefined),

                    Caller = rfc4627:get_field(P2PConfInfo, "caller", undefined),
                    CallerDevType_ = rfc4627:get_field(Caller, "devtype", undefined),
                    lager:info("  -->  P2P CallerType = ~p~n", [CallerDevType_]),
                    CallerType = binary_to_list(CallerDevType_),
                    CallerDevName_ = rfc4627:get_field(Caller, "devname", undefined),
                    lager:info("  -->  P2P CallerName = ~p~n", [CallerDevName_]),
                    CallerName = binary_to_list(CallerDevName_),
                    CallerDevE164_ = rfc4627:get_field(Caller, "deve164", undefined),
                    lager:info("  -->  P2P CallerE164 = ~p~n", [CallerDevE164_]),
                    CallerE164 = binary_to_list(CallerDevE164_),

                    Callee = rfc4627:get_field(P2PConfInfo, "callee", undefined),
                    CalleeDevType_ = rfc4627:get_field(Callee, "devtype", undefined),
                    lager:info("  -->  P2P CalleeType = ~p~n", [CalleeDevType_]),
                    CalleeType = binary_to_list(CalleeDevType_),
                    CalleeDevName_ = rfc4627:get_field(Callee, "devname", undefined),
                    lager:info("  -->  P2P CalleeName = ~p~n", [CalleeDevName_]),
                    CalleeName = binary_to_list(CalleeDevName_),
                    CalleeDevE164_ = rfc4627:get_field(Callee, "deve164", undefined),
                    lager:info("  -->  P2P CalleeE164 = ~p~n", [CalleeDevE164_]),
                    CalleeE164 = binary_to_list(CalleeDevE164_),

                    ConfStartTime_ = rfc4627:get_field(P2PConfInfo, "time", undefined),
                    lager:info("  -->  P2P ConfStartTime = ~p~n", [ConfStartTime_]),
                    ConfStartTime = binary_to_list(ConfStartTime_),

                    ConfBitRate = rfc4627:get_field(P2PConfInfo, "bitrate", undefined),
                    lager:info("  -->  P2P ConfBitRate = ~p~n", [ConfBitRate]),

                    %% 从 redis 的 HASH 表 terminal:callerE164:baseinfo 和 terminal:calleeE164:baseinfo 中查 domain_moid
                    case gen_server:call(RedisTask, {get_terminal_base_info_by_e164, CallerE164}, infinity) of
                        {error, PasP2PConfCreateErr0} ->
                            lager:warning("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Failed! Error '~p'~n", 
                                [CallerE164, PasP2PConfCreateErr0]),
                            lager:warning("[nms_task_control] Find no terminal info by E164, Make sure E164(~p) is Correct!~n",
                                [CallerE164]),
                            CallerDomainMoid_ = <<"match no domain">>;
                        {ok, {_,CallerDomainMoid_,_,_}} ->
                            lager:info("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Success! CallerDomainMoid=~p~n", 
                                [CallerE164, CallerDomainMoid_])
                    end,
                    CallerDomainMoid = binary_to_list(CallerDomainMoid_),

                    case gen_server:call(RedisTask, {get_terminal_base_info_by_e164, CalleeE164}, infinity) of
                        {error, PasP2PConfCreateErr1} ->
                            lager:warning("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Failed! Error '~p'~n", 
                                [CalleeE164, PasP2PConfCreateErr1]),
                            lager:warning("[nms_task_control] Find no terminal info by E164, Make sure E164(~p) is Correct!~n",
                                [CalleeE164]),
                            CalleeDomainMoid_ = <<"match no domain">>;
                        {ok, {_,CalleeDomainMoid_,_,_}} ->
                            lager:info("[nms_task_control] 'HGETALL terminal:~p:baseinfo' -- Success! CalleeDomainMoid=~p~n", 
                                [CalleeE164, CalleeDomainMoid_])
                    end,
                    CalleeDomainMoid = binary_to_list(CalleeDomainMoid_),

                    %% 更新 redis 的 SET 表 domain:callerUserDomainid:p2p_meeting 和 domain:calleeUserDomainid:p2p_meeting
                    %% 更新 redis 的 HASH 表 p2p_meeting:callerE164:info
                    case gen_server:call(RedisTask, {add_p2p_meeting, CallerDomainMoid, CallerE164, CallerName, CallerType, 
                            CalleeDomainMoid, CalleeE164, CalleeName, CalleeType, ConfBitRate, ConfStartTime}, infinity) of
                        {error, PasP2PConfCreateErr2} ->
                            lager:warning("[nms_task_control] 'MULTI'~n", []),
                            lager:warning("[nms_task_control] 'SADD domain:~p:p2p_meeting ~p~n", [CallerDomainMoid, CallerE164]),
                            lager:warning("[nms_task_control] 'SADD domain:~p:p2p_meeting ~p~n", [CalleeDomainMoid, CallerE164]),
                            lager:warning("[nms_task_control] 'HMSET p2p_meeting:~p:info~n", [CallerE164]),
                            lager:warning("[nms_task_control] 'EXEC' -- Failed! Error '~p'~n", [PasP2PConfCreateErr2]);

                        {ok, _} ->
                            lager:info("[nms_task_control] 'MULTI'~n", []),
                            lager:info("[nms_task_control] 'SADD domain:~p:p2p_meeting ~p~n", [CallerDomainMoid, CallerE164]),
                            lager:info("[nms_task_control] 'SADD domain:~p:p2p_meeting ~p~n", [CalleeDomainMoid, CallerE164]),
                            lager:info("[nms_task_control] 'HMSET p2p_meeting:~p:info~n", [CallerE164]),
                            lager:info("[nms_task_control] 'EXEC' -- Success!~n", [])
                    end,

                    io:format("", []);

                <<"EV_PAS_P2PCONF_DESTROY">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"PAS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"confinfo",
                    %%           {obj,[{"callerE164",<<"0512111885803">>},
                    %%                 {"calleeE164",<<"0512111885804">>}]}},
                    %%       {"eventid",<<"EV_PAS_P2PCONF_DESTROY">>}]}

                    lager:info("[nms_task_control] get 'EV_PAS_P2PCONF_DESTROY' event!~n", []),

                    P2PConfInfo = rfc4627:get_field(JsonObj, "confinfo", undefined),

                    CallerE164_ = rfc4627:get_field(P2PConfInfo, "callerE164", undefined),
                    lager:info("  -->  P2P CallerE164 = ~p~n", [CallerE164_]),
                    CallerE164 = binary_to_list(CallerE164_),

                    case gen_server:call(RedisTask, {del_p2p_meeting, CallerE164}, infinity) of
                        {error, PasP2PConfDestroyErr0} ->
                            lager:warning("[nms_task_control] 'MULTI'~n", []),
                            lager:warning("[nms_task_control] 'SREM domain:CallerDomainMoid:p2p_meeting ~p~n", [CallerE164]),
                            lager:warning("[nms_task_control] 'SREM domain:CalleeDomainMoid:p2p_meeting ~p~n", [CallerE164]),
                            lager:warning("[nms_task_control] 'DEL p2p_meeting:~p:info~n", [CallerE164]),
                            lager:warning("[nms_task_control] 'EXEC' -- Failed! Error '~p'~n", [PasP2PConfDestroyErr0]);

                        {ok, _} ->
                            lager:info("[nms_task_control] 'MULTI'~n", []),
                            lager:info("[nms_task_control] 'SREM domain:CallerDomainMoid:p2p_meeting ~p~n", [CallerE164]),
                            lager:info("[nms_task_control] 'SREM domain:CalleeDomainMoid:p2p_meeting ~p~n", [CallerE164]),
                            lager:info("[nms_task_control] 'DEL p2p_meeting:~p:info~n", [CallerE164]),
                            lager:info("[nms_task_control] 'EXEC' -- Success!~n", [])
                    end,

                    io:format("", []);

                <<"EV_PAS_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1111">>},
                    %%       {"devtype",<<"PAS">>},
                    %%       {"rpttime",<<"2014/06/16:09:57:50">>},
                    %%       {"version",<<"1.06">>},
                    %%       {"pidchange",0},
                    %%       {"pasinfo",
                    %%           {obj,[{"h323onlinecount",123},
                    %%                 {"siponlinecount",123},
                    %%                 {"monitoronlinecount",123},
                    %%                 {"callingcount",123},
                    %%                 {"maxcallcount",1234},
                    %%                 {"confmtcount",123},
                    %%                 {"curonlinecount",123},
                    %%                 {"maxonlinecount",123}]}},
                    %%       {"eventid",<<"EV_PAS_INFO">>}]}

                    lager:info("[nms_task_control] get 'EV_PAS_INFO' event!~n", []),

                    PASVersion_ = rfc4627:get_field(JsonObj, "version", undefined),
                    lager:info("  -->  PAS Version = ~p~n", [PASVersion_]),

                    PidChange_ = rfc4627:get_field(JsonObj, "pidchange", undefined),
                    lager:info("  -->  PAS PidChanged = ~p~n", [PidChange_]),

                    PASInfo = rfc4627:get_field(JsonObj, "pasinfo", undefined),

                    H323Online = rfc4627:get_field(PASInfo, "h323onlinecount", undefined),
                    lager:info("  -->  H323Online = ~p~n", [H323Online]),

                    SIPOnline = rfc4627:get_field(PASInfo, "siponlinecount", undefined),
                    lager:info("  -->  SIPOnline = ~p~n", [SIPOnline]),

                    MonitorOnline = rfc4627:get_field(PASInfo, "monitoronlinecount", undefined),
                    lager:info("  -->  MonitorOnline = ~p~n", [MonitorOnline]),

                    P2PCallPair = rfc4627:get_field(PASInfo, "callingcount", undefined),
                    lager:info("  -->  P2PCallPair = ~p~n", [P2PCallPair]),

                    MaxP2PCallPair = rfc4627:get_field(PASInfo, "maxcallcount", undefined),
                    lager:info("  -->  MaxP2PCallPair = ~p~n", [MaxP2PCallPair]),

                    P2PMeetingMTNum = rfc4627:get_field(PASInfo, "confmtcount", undefined),
                    lager:info("  -->  P2PMeetingMTNum = ~p~n", [P2PMeetingMTNum]),

                    PidChangeTriggered = case PidChange_ of
                        0 -> %% 进程未异常
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(pas_pid_change_enum_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(PidChangeTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?L_SERVER,StatisticTime,EventID,2011), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(PidChangeTriggered,RedisTask,DevMoid,?L_SERVER,2011), 

        %% ------------------------------------------------------------

                    %% 更新 redis 的 HASH 表 domain:PlatformDomainMoid:pas:pas_moid
                    case gen_server:call(RedisTask, 
                            {add_pas_online_statistic, PlatformDomainMoid, DevMoid, H323Online, SIPOnline, MonitorOnline}, infinity) of
                        {error, PasInfoErr1} ->
                            lager:warning("[nms_task_control] 'HMSET domain:~p:pas:~p' -- Failed! Error '~p'~n", 
                                [PlatformDomainMoid, DevMoid, PasInfoErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET domain:~p:pas:~p' -- Success!~n", [PlatformDomainMoid, DevMoid])
                    end,

                    %% 向 redis 的 SET 表 pas_in_all_domains 中保存 domain:PlatformDomainMoid:pas:pas_moid 字符串
                    CompletePasDomainInfo = "domain:" ++ PlatformDomainMoid ++ ":pas:" ++ DevMoid,
                    case gen_server:call(RedisTask, {add_pas_in_all_domains, CompletePasDomainInfo}, infinity) of
                        {error, PasInfoErr2} ->
                            lager:warning("[nms_task_control] 'SADD pas_in_all_domains ~p' -- Failed! Error '~p'~n", 
                                [CompletePasDomainInfo, PasInfoErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD pas_in_all_domains ~p' -- Success!~n", [CompletePasDomainInfo])
                    end,

                    io:format("", []);

                OtherEvent     ->
                    lager:info("[nms_task_control] get '~p' event, do nothing!~n", [OtherEvent])
            end;

        {error,<<"Key Error">>} ->
            lager:warning("[nms_task_control] get <<\"Key Error\">> for key ~p~n", [DevGuid]),
            throw(redis_key_error);

        {error, no_connection} ->
            lager:error("[nms_task_control] lost redis connection!"),
            throw(redis_connection_lost)
    end.


terminal_device_proc(JsonObj, RedisTask, MySQLTask, Type) ->
    DevMoid_ = rfc4627:get_field(JsonObj, "devid", undefined),
    lager:info("  -->  DevMoid = ~p~n", [DevMoid_]),
    DevMoid = binary_to_list(DevMoid_),

    %% rpttime 格式 year-month-day/hour:min:sec
    StatisticTime_ = rfc4627:get_field(JsonObj, "rpttime", undefined),
    case StatisticTime_ of
        undefined ->
            {{Year,Month,Day},{Hour,Min,Sec}} = calendar:now_to_local_time(os:timestamp()),
            StatisticTime = integer_to_list(Year)++"/"++integer_to_list(Month)++"/"++integer_to_list(Day)++":"++
                            integer_to_list(Hour)++":"++integer_to_list(Min)++":"++integer_to_list(Sec),
            lager:info("[nms_task_control] find no rpttime, so make it myself:~p~n", 
                [StatisticTime]);
        _ ->
            StatisticTime = binary_to_list(StatisticTime_)
    end,
    lager:info("  -->  StatisticTime = ~p~n", [StatisticTime]),

    EventID = rfc4627:get_field(JsonObj, "eventid", undefined),
    lager:info("  -->  EventID = ~p~n", [EventID]),

    %% 通过 "devid" 在 redis 中查询当前终端设备所属的域 ID
    case gen_server:call(RedisTask, {get_terminal_base_info, DevMoid_}, infinity) of
        { ok, {DevMoid_, DomainMoid_, DevName_, E164_} } ->

            lager:info("  -->  DomainMoid = ~p~n", [DomainMoid_]),
            lager:info("  -->  DevName = ~p~n", [DevName_]),
            lager:info("  -->  E164 = ~p~n", [E164_]),

            DevMoid    = binary_to_list(DevMoid_),
            DomainMoid = binary_to_list(DomainMoid_),
            E164       = binary_to_list(E164_),

            DevType_ = rfc4627:get_field(JsonObj, "devtype", undefined),
            DevType = binary_to_list(DevType_),

            %% 通过 "eventid" 判定为信息类型
            case EventID of
                <<"EV_DEV_ONLINE">>     ->
                    %% 消息举例
                    %% {obj,[{"eventid",<<"EV_DEV_ONLINE">>},
                    %%       {"devid",<<"1.2.1">>},
                    %%       {"devtype",<<"SERVICE_KDV_MT_TS6610">>},
                    %%       {"collectorid",<<"60a44c502a60">>}]}
                    %% 
                    lager:info("[nms_task_control] get 'EV_DEV_ONLINE' event!~n", []),

                    CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
                    lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
                    CollectorID = binary_to_list(CollectorID_),

                    %%  设置 Redis 表 terminal:devid:online 的值为 online
                    case gen_server:call(RedisTask, {add_terminal_online, DevMoid}, infinity) of
                        {error, TerDevOnlineErr0} ->
                            lager:warning("[nms_task_control] 'SET terminal:~p:online online' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevOnlineErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SET terminal:~p:online online' -- Success!~n", [DevMoid])
                    end,

                    %% 向 Redis 的 SET 表 collector 中保存 collectorid
                    case gen_server:call(RedisTask, {add_collectorid, CollectorID}, infinity) of
                        {error, TerDevOnlineErr1} ->
                            lager:warning("[nms_task_control] 'SADD collector ~p' -- Failed! Error '~p'~n", 
                                [CollectorID, TerDevOnlineErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD collector ~p' -- Success!~n", [CollectorID])
                    end,

                    %% 向 Redis 表 collector:collectorid:online 中写入 devtype:devid 信息
                    case gen_server:call(RedisTask, {add_collector_online_device, CollectorID, DevMoid, DevType}, infinity) of
                        {error, TerDevOnlineErr2} ->
                            lager:warning("[nms_task_control] 'SADD collector:~p:online' -- Failed! Error '~p'~n", 
                                [CollectorID, TerDevOnlineErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SADD collector:~p:online ~p:~p' -- Success!~n", 
                                [CollectorID, DevType, DevMoid])
                    end,
                    io:format("", []);

                <<"EV_DEV_OFFLINE">>     ->
                    %% 消息举例
                    %% {obj,[{"eventid",<<"EV_DEV_OFFLINE">>},
                    %%       {"devid",<<"1111">>},
                    %%       {"devtype",<<"SERVICE_TS_SRV_MPCD">>},
                    %%       {"collectorid",<<"60a44c502a60">>}]}
                    %% 
                    lager:info("[nms_task_control] get 'EV_DEV_OFFLINE' event!~n", []),

                    CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
                    lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
                    CollectorID = binary_to_list(CollectorID_),

                    %%  删除 Redis 表 terminal:devid:online
                    case gen_server:call(RedisTask, {del_terminal_online, DevMoid}, infinity) of
                        {error, TerDevOfflineErr0} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:online' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevOfflineErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:online' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 terminal:devid:connection
                    case gen_server:call(RedisTask, {del_terminal_connections, DevMoid}, infinity) of
                        {error, TerDevOfflineErr1} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:connection' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevOfflineErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:connection' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 terminal:devid:resource
                    case gen_server:call(RedisTask, {del_terminal_resource, DevMoid}, infinity) of
                        {error, TerDevOfflineErr2} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:resource' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevOfflineErr2]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:resource' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 terminal:devid:warning
                    case gen_server:call(RedisTask, {del_terminal_warning_all, DevMoid}, infinity) of
                        {error, TerDevOfflineErr3} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:warning' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevOfflineErr3]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:warning' -- Success!~n", [DevMoid])
                    end,

                    %%  删除 Redis 表 terminal:devid:runninginfo
                    case gen_server:call(RedisTask, {del_terminal_running_info, DevMoid}, infinity) of
                        {error, TerDevOfflineErr4} ->
                            lager:warning("[nms_task_control] 'DEL terminal:~p:runninginfo' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevOfflineErr4]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'DEL terminal:~p:runninginfo' -- Success!~n", [DevMoid])
                    end,

                    %% 从 Redis 表 collector:collectorid:online 中删除 devtype:devid 信息
                    case gen_server:call(RedisTask, {del_collector_online_device, CollectorID, DevMoid, DevType}, infinity) of
                        {error, TerDevOfflineErr5} ->
                            lager:warning("[nms_task_control] 'SREM collector:~p:online' -- Failed! Error '~p'~n", 
                                [CollectorID, TerDevOfflineErr5]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'SREM collector:~p:online ~p:~p' -- Success!~n", 
                                [CollectorID, DevType, DevMoid])
                    end,
                    io:format("", []);

                <<"EV_MT_INFO">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"mt_info",
                    %%           {obj,[{"aps_addr",
                    %%                     {obj,[{"domain",<<"172.16.72.84">>},
                    %%                           {"ip",<<"218.22.22.23">>}]}},
                    %%                 {"cpu_type",<<"i7">>},
                    %%                 {"cpu_freq",5},
                    %%                 {"cpu_num",4},
                    %%                 {"devname",<<"longweitest">>},
                    %%                 {"devver",<<"20141215">>},
                    %%                 {"oem",<<"dddd">>},
                    %%                 {"memory",4000},
                    %%                 {"netinfo",
                    %%                     {obj,[{"dns",<<"172.16.0.65">>},
                    %%                           {"nat_ip",<<"172.16.72.84">>},
                    %%                           {"ip",<<"172.16.72.84">>}]}},
                    %%                 {"os",<<"XP">>}]}},
                    %%       {"eventid",<<"EV_MT_INFO">>}]}
                    lager:info("[nms_task_control] get 'EV_MT_INFO' event!~n", []),

                    MtInfo = rfc4627:get_field(JsonObj, "mt_info", undefined),

                    APSAddr = rfc4627:get_field(MtInfo, "aps_addr", undefined),

                    APSDomainName_ = rfc4627:get_field(APSAddr, "domain", undefined),
                    lager:info("  -->  APSDomainName = ~p~n", [APSDomainName_]),
                    APSDomainName = binary_to_list(APSDomainName_),

                    APSIp_ = rfc4627:get_field(APSAddr, "ip", undefined),
                    lager:info("  -->  APSIp = ~p~n", [APSIp_]),
                    APSIp = binary_to_list(APSIp_),

                    NetInfo = rfc4627:get_field(MtInfo, "netinfo", undefined),

                    TerDns_ = rfc4627:get_field(NetInfo, "dns", undefined),
                    lager:info("  -->  TerDns = ~p~n", [TerDns_]),
                    TerDns = binary_to_list(TerDns_),

                    TerNatIp_ = rfc4627:get_field(NetInfo, "nat_ip", undefined),
                    lager:info("  -->  TerNatIp = ~p~n", [TerNatIp_]),
                    TerNatIp = binary_to_list(TerNatIp_),

                    TerIp_ = rfc4627:get_field(NetInfo, "ip", undefined),
                    lager:info("  -->  TerIp = ~p~n", [TerIp_]),
                    TerIp = binary_to_list(TerIp_),

                    TerCpuNum_ = rfc4627:get_field(MtInfo, "cpu_num", undefined),
                    lager:info("  -->  TerCpuNum = ~p~n", [TerCpuNum_]),
                    TerCpuNum = integer_to_list(TerCpuNum_), 

                    %% 这里需要再确认，因为 baseinfo 里已经有 name 字段
                    %% 周结论：设备名仅在 baseinfo 中存在，且为事先导入的，其余消息中设备名都不需要
                    %%DevName_ = rfc4627:get_field(MtInfo, "devname", undefined),
                    %%DevName = binary_to_list(DevName_),

                    %% 此处的 oem 信息是不需要的，存在于 EV_VERSION_MSG 消息中的 oem 才是需要的
                    %%OEM_ = rfc4627:get_field(MtInfo, "oem", undefined),
                    %%OEM = binary_to_list(OEM_),

                    DevVer_ = rfc4627:get_field(MtInfo, "devver", undefined),
                    lager:info("  -->  DevVer = ~p~n", [DevVer_]),
                    DevVer = binary_to_list(DevVer_),

                    OS_ = rfc4627:get_field(MtInfo, "os", undefined),
                    lager:info("  -->  OS = ~p~n", [OS_]),
                    OS = binary_to_list(OS_),

                    CpuType_ = rfc4627:get_field(MtInfo, "cpu_type", undefined),
                    lager:info("  -->  CpuType = ~p~n", [CpuType_]),
                    CpuType = binary_to_list(CpuType_),

                    CpuFreq_ = rfc4627:get_field(MtInfo, "cpu_freq", undefined),
                    lager:info("  -->  CpuFreq = ~p(MHZ)~n", [CpuFreq_]),
                    CpuFreq = integer_to_list(CpuFreq_)++"MHZ",

                    Memory_ = rfc4627:get_field(MtInfo, "memory", undefined),
                    lager:info("  -->  Memory = ~p(MB)~n", [Memory_]),

                    MemoryGB = Memory_ / 1024,
                    MemoryGBList = erlang:float_to_list(MemoryGB, [{decimals, 2}])++"GB",

                    %% 向 Redis 的 terminal:devid:runninginfo 表中保存终端 运行时信息
                    case gen_server:call(RedisTask, {add_terminal_running_info, DevMoid, atom_to_list(Type), DevVer, 
                            OS, CpuType, CpuFreq, TerCpuNum, MemoryGBList}, infinity) of
                        {error, TerDevInfoErr0} ->
                            lager:warning("[nms_task_control] 'HMSET terminal:~p:runninginfo' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevInfoErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET terminal:~p:runninginfo' -- Success!~n", [DevMoid])
                    end,

                    %% 向 Redis 的 terminal:devid:netinfo 表中保存终端 APS 和网络相关信息
                    case gen_server:call(RedisTask, {add_terminal_aps_and_net_info, DevMoid, TerIp, TerNatIp, TerDns, 
                            APSDomainName, APSIp}, infinity) of
                        {error, TerDevInfoErr1} ->
                            lager:warning("[nms_task_control] 'HMSET terminal:~p:netinfo' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevInfoErr1]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET terminal:~p:netinfo' -- Success!~n", [DevMoid])
                    end,

                    io:format("", []);

                <<"EV_NETINFO_MSG">>   ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"netinfo",
                    %%           {obj,[{"dns",<<"172.16.0.65">>},
                    %%                 {"nat_ip",<<"172.16.72.84">>},
                    %%                 {"ip",<<"172.16.72.84">>}]}},
                    %%       {"eventid",<<"EV_NETINFO_MSG">>}]}

                    lager:info("[nms_task_control] get 'EV_NETINFO_MSG' event!~n", []),

                    NetInfo = rfc4627:get_field(JsonObj, "netinfo", undefined),

                    TerDns_ = rfc4627:get_field(NetInfo, "dns", undefined),
                    lager:info("  -->  TerDns = ~p~n", [TerDns_]),
                    TerDns = binary_to_list(TerDns_),

                    TerNatIp_ = rfc4627:get_field(NetInfo, "nat_ip", undefined),
                    lager:info("  -->  TerNatIp = ~p~n", [TerNatIp_]),
                    TerNatIp = binary_to_list(TerNatIp_),

                    TerIp_ = rfc4627:get_field(NetInfo, "ip", undefined),
                    lager:info("  -->  TerIp = ~p~n", [TerIp_]),
                    TerIp = binary_to_list(TerIp_),

                    %% 向 Redis 的 terminal:devid:netinfo 表中保存终端 网络相关信息
                    case gen_server:call(RedisTask, {add_terminal_net_info, DevMoid, TerIp, TerNatIp, TerDns}, infinity) of
                        {error, TerDevNetInfoErr0} ->
                            lager:warning("[nms_task_control] 'HMSET terminal:~p:netinfo' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevNetInfoErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET terminal:~p:netinfo' -- Success!~n", [DevMoid])
                    end,

                    io:format("", []);

                <<"EV_BANDWIDTH_MSG">>   ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"recv_bandwidth",
                    %%           {obj,[{"bandwidth",1024},
                    %%                 {"drop_rate",10}]}},
                    %%       {"send_bandwidth",
                    %%           {obj,[{"bandwidth",512},
                    %%                 {"drop_rate",3}]}},
                    %%       {"eventid",<<"EV_BANDWIDTH_MSG">>}]}

                    lager:info("[nms_task_control] get 'EV_BANDWIDTH_MSG' event!~n", []),

                    RBW = rfc4627:get_field(JsonObj, "recv_bandwidth", undefined),

                    RecvBandWidth_ = rfc4627:get_field(RBW, "bandwidth", undefined),
                    lager:info("  -->  RecvBandWidth = ~p~n", [RecvBandWidth_]),
                    RecvBandWidth = integer_to_list(RecvBandWidth_), 

                    RecvDropRate_ = rfc4627:get_field(RBW, "drop_rate", undefined),
                    lager:info("  -->  RecvDropRate = ~p~n", [RecvDropRate_]),
                    RecvDropRate = integer_to_list(RecvDropRate_),

                    SBW = rfc4627:get_field(JsonObj, "send_bandwidth", undefined),

                    SendBandWidth_ = rfc4627:get_field(SBW, "bandwidth", undefined),
                    lager:info("  -->  SendBandWidth = ~p~n", [SendBandWidth_]),
                    SendBandWidth = integer_to_list(SendBandWidth_), 

                    SendDropRate_ = rfc4627:get_field(SBW, "drop_rate", undefined),
                    lager:info("  -->  SendDropRate = ~p~n", [SendDropRate_]),
                    SendDropRate = integer_to_list(SendDropRate_),

                    %% 向 Redis 的 terminal:devid:netinfo 表中保存终端 带宽相关信息
                    case gen_server:call(RedisTask, {add_terminal_bandwidth_info, 
                            DevMoid, SendBandWidth, SendDropRate, RecvBandWidth, RecvDropRate}, infinity) of
                        {error, TerDevBandwidthInfoErr0} ->
                            lager:warning("[nms_task_control] 'HMSET terminal:~p:netinfo' -- Failed! Error '~p'~n", 
                                [DevMoid, TerDevBandwidthInfoErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET terminal:~p:netinfo' -- Success!~n", [DevMoid])
                    end,

                    io:format("", []);

                <<"EV_VERSION_MSG">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"SERVICE_TS_MT_TRUELINK">>},
                    %%       {"devver",<<"20141215">>},
                    %%       {"oem",<<"dddd">>},
                    %%       {"recommend",1},
                    %%       {"eventid",<<"EV_VERSION_MSG">>}]}
                    lager:info("[nms_task_control] get 'EV_VERSION_MSG' event!~n", []),

                    Oem_ = rfc4627:get_field(JsonObj, "oem", undefined),
                    lager:info("  -->  Oem = ~p~n", [Oem_]),
                    Oem = binary_to_list(Oem_),

                    Version_ = rfc4627:get_field(JsonObj, "devver", undefined),
                    lager:info("  -->  Version = ~p~n", [Version_]),
                    Version = binary_to_list(Version_),

                    Recommend_ = rfc4627:get_field(JsonObj, "recommend", undefined),
                    lager:info("  -->  Recommend = ~p~n", [Recommend_]),
                    Recommend = integer_to_list(Recommend_),

                    %% 先通过 devid 和 type 来确定 MySQL 表 terminal_version_statistic 中是否存在相应信息
                    %% 若不存在，则插入版本相关信息；若存在，则更新版本相关信息（该逻辑接口内容已实现）
                    case gen_server:call(MySQLTask, 
                            {add_terminal_version_statistic,DomainMoid,DevMoid,E164,atom_to_list(Type),Oem,Version,Recommend}, 
                            infinity) of
                        {ok, success} ->
                            lager:info("[nms_task_control] 'INSERT INTO terminal_version_statistic' -- Success!~n", []);
                        _ ->
                            lager:warning("[nms_task_control] 'INSERT INTO terminal_version_statistic' -- Failed!~n", [])
                    end,                    
                    io:format("", []);

                <<"EV_PFMINFO_MSG">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"pfm_info",
                    %%           {obj,[{"cpu_userate",50},
                    %%                 {"mem_userate",85},
                    %%                 {"disk_userate",75}]}},
                    %%       {"eventid",<<"EV_PFMINFO_MSG">>}]}
                    lager:info("[nms_task_control] get 'EV_PFMINFO_MSG' event!~n", []),

                    PFMInfo = rfc4627:get_field(JsonObj, "pfm_info", undefined),

                    CpuPct_ = rfc4627:get_field(PFMInfo, "cpu_userate", undefined),
                    lager:info("  -->  CpuPct = ~p~n", [CpuPct_]),
                    CpuPct = integer_to_list(CpuPct_),

                    MemPct_ = rfc4627:get_field(PFMInfo, "mem_userate", undefined),
                    lager:info("  -->  MemPct = ~p~n", [MemPct_]),
                    MemPct = integer_to_list(MemPct_),

                    DiskPct_ = rfc4627:get_field(PFMInfo, "disk_userate", undefined),
                    lager:info("  -->  DiskPct = ~p~n", [DiskPct_]),
                    DiskPct = integer_to_list(DiskPct_),

                    %% 向 Redis 表 terminal:devid:resource 中插入信息
                    case gen_server:call(RedisTask, {add_terminal_resource, DevMoid, CpuPct, DiskPct, MemPct}, infinity) of
                        {error, TerPFMInfoErr0} ->
                            lager:warning("[nms_task_control] 'HMSET terminal:~p:resource' -- Failed! Error '~p'~n", 
                                [DevMoid, TerPFMInfoErr0]);
                        {ok, _} ->
                            lager:info("[nms_task_control] 'HMSET terminal:~p:resource' -- Success!~n", [DevMoid])
                    end,
                    io:format("", []); 

                <<"EV_SHOULD_CONNSRV_MSG">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"conn_srv_type_info",[
                    %%           <<"APS">>,
                    %%           <<"PAS">>,
                    %%           <<"SUS">>]},
                    %%       {"eventid",<<"EV_SHOULD_CONNSRV_MSG">>}]}
                    lager:info("[nms_task_control] get 'EV_SHOULD_CONNSRV_MSG' event!~n", []),

                    ConSrvTypeInfo_ = rfc4627:get_field(JsonObj, "conn_srv_type_info", undefined),
                    ConSrvTypeInfo = lists:map( fun(Srv) -> binary_to_list(Srv) end, ConSrvTypeInfo_ ),

                    %% 向 redis 表 terminal:devid:connection 中保存需要连接服务器的 ip 信息（将 IP 置空）
                    case gen_server:call(RedisTask, {add_terminal_connections, DevMoid, ConSrvTypeInfo}, infinity) of
                        {error, ShConSrvErr0} ->
                            lager:warning("[nms_task_control] '(Pipeline)HSET terminal:~p:connection' -- Failed! Error '~p'~n", 
                                [DevMoid, ShConSrvErr0]);
                        _ ->
                            lager:info("[nms_task_control] '(Pipeline)HSET terminal:~p:connection' -- Success!~n", [DevMoid])
                    end,

                    io:format("", []);

                <<"EV_CONNSRV_CONN_MSG">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"conn_srv_state_info", [
                    %%            {obj,[{"ip",<<"172.16.72.88">>},
                    %%                  {"type",<<"APS">>}]},
                    %%            {obj,[{"ip",<<"172.16.72.89">>},
                    %%                  {"type",<<"PAS">>}]},
                    %%            {obj,[{"ip",<<"172.16.72.82">>},
                    %%                  {"type",<<"SUS">>}]}]},
                    %%       {"eventid",<<"EV_CONNSRV_CONN_MSG">>}]}
                    lager:info("[nms_task_control] get 'EV_CONNSRV_CONN_MSG' event!~n", []),

                    ConSrvStateInfo = rfc4627:get_field(JsonObj, "conn_srv_state_info", undefined),

                    %% 构造 ServerConInfoList 元组列表
                    ServerConInfoList = [ {binary_to_list(ConSrvType),binary_to_list(IP)} || 
                        {obj,[{"ip",IP},{"type",ConSrvType}]} <- ConSrvStateInfo ],

                    %% 向 redis 表 terminal:devid:connection 中更新服务器 ip 信息
                    case gen_server:call(RedisTask, {update_terminal_connections, DevMoid, ServerConInfoList}, infinity) of
                        {error, ConSrvErr0} ->
                            lager:warning("[nms_task_control] '(Pipeline)HSET terminal:~p:connection' -- Failed! Error '~p'~n", 
                                [DevMoid, ConSrvErr0]);
                        _ ->  %% [{ok,<<"0">>},...]
                            lager:info("[nms_task_control] '(Pipeline)HSET terminal:~p:connection' -- Success!~n", [DevMoid])
                    end,

                    io:format("", []); 

                <<"EV_CONNSRV_DESC_MSG">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"conn_srv_type_info",[
                    %%           <<"APS">>,
                    %%           <<"PAS">>,
                    %%           <<"SUS">>]},
                    %%       {"eventid",<<"EV_CONNSRV_DESC_MSG">>}]}
                    lager:info("[nms_task_control] get 'EV_CONNSRV_DESC_MSG' event!~n", []),

                    DiscSrvTypeInfo_ = rfc4627:get_field(JsonObj, "conn_srv_type_info", undefined),
                    DiscSrvTypeInfo = lists:map( fun(DiscSrv) -> binary_to_list(DiscSrv) end, DiscSrvTypeInfo_ ),

                    %% 向 redis 表 terminal:devid:connection 中保存需要连接服务器的 ip 信息（将 IP 置空）
                    case gen_server:call(RedisTask, {add_terminal_connections, DevMoid, DiscSrvTypeInfo}, infinity) of
                        {error, DiscSrvErr0} ->
                            lager:warning("[nms_task_control] '(Pipeline)HSET terminal:~p:connection' -- Failed! Error '~p'~n", 
                                [DevMoid, DiscSrvErr0]);
                        _ ->
                            lager:info("[nms_task_control] '(Pipeline)HSET terminal:~p:connection' -- Success!~n", [DevMoid])
                    end,

                    io:format("", []); 

                <<"EV_ALARM_MSG">>     ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"alarm_info",
                    %%           {obj,[{"code_id",1003},
                    %%                 {"start_time",<<"2014/05/08:12:08:08">>},
                    %%                 {"status",0}]}},
                    %%       {"eventid",<<"EV_ALARM_MSG">>}]}
                    lager:info("[nms_task_control] get 'EV_ALARM_MSG' event!~n", []),

                    AlarmInfo = rfc4627:get_field(JsonObj, "alarm_info", undefined),

                    WarningCode = rfc4627:get_field(AlarmInfo, "code_id", undefined),
                    lager:info("  -->  WarningCode = ~p~n", [WarningCode]),

                    TerRptTime_ = rfc4627:get_field(AlarmInfo, "start_time", undefined),
                    lager:info("  -->  TerRptTime = ~p~n", [TerRptTime_]),
                    TerRptTime = binary_to_list(TerRptTime_),

                    Status_ = rfc4627:get_field(AlarmInfo, "status", undefined),
                    lager:info("  -->  Status = ~p~n", [Status_]),
                    TerWarningTriggered = case Status_ of
                        0 -> %% 告警恢复
                            false;
                        1 ->
                            true;
                        _ ->
                            throw(terminal_status_error)
                    end,

        %% ------------------------------------------------------------
                    %% 未处理失败
                    update_mysql_warning_info(TerWarningTriggered,MySQLTask,DevMoid,DomainMoid,
                        ?TERMINAL,TerRptTime,EventID,WarningCode), 

        %% ------------------------------------------------------------

                    update_redis_warning_info(TerWarningTriggered,RedisTask,DevMoid,?TERMINAL,WarningCode), 

        %% ------------------------------------------------------------

                    io:format("", []);

                <<"EV_EXCEPTION_FILE">>   ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"execption_info",
                    %%           {obj,[{"devver",<<"2.6.0.21">>},
                    %%                 {"execption_file",<<"moooofly.err">>},
                    %%                 {"execption_time",<<"2014/05/08:12:08:08">>}]}},
                    %%       {"eventid",<<"EV_EXCEPTION_FILE">>}]}
                    lager:info("[nms_task_control] get 'EV_EXCEPTION_FILE' event!~n", []),

                    ExecptionInfo = rfc4627:get_field(JsonObj, "execption_info", undefined),

                    DevVer_ = rfc4627:get_field(ExecptionInfo, "devver", undefined),
                    lager:info("  -->  DevVer = ~p~n", [DevVer_]),
                    %%DevVer = binary_to_list(DevVer_),

                    ExecFile_ = rfc4627:get_field(ExecptionInfo, "execption_file", undefined),
                    lager:info("  -->  ExecFile = ~p~n", [ExecFile_]),
                    %%ExecFile = binary_to_list(ExecFile_),

                    ExecTime_ = rfc4627:get_field(ExecptionInfo, "execption_time", undefined),
                    lager:info("  -->  ExecTime = ~p~n", [ExecTime_]),
                    %%ExecTime = binary_to_list(ExecTime_),

                    io:format("", []);
            
                <<"EV_CONF_INFO">>        ->
                    %% 消息举例
                    %% {obj,[{"devid",<<"1.2.2">>},
                    %%       {"devtype",<<"Skywalker for Windows">>},
                    %%       {"mt_state",2},
                    %%       {"conf_info",
                    %%           {obj,[{"mt_e164",<<"0512111886042">>},
                    %%                 {"conf_e164",<<"0512111886042">>},
                    %%                 {"bitrate",328},
                    %%                 {"privideo_send", [
                    %%                     {obj,[{"id",1},
                    %%                           {"video_resource_exist",1},
                    %%                           {"format",0},
                    %%                           {"framerate",25},
                    %%                           {"video_up_bitrate",256}]},
                    %%                     {obj,[{"id",2},
                    %%                           {"video_resource_exist",1},
                    %%                           {"format",0},
                    %%                           {"framerate",25},
                    %%                           {"video_up_bitrate",256}]},
                    %%                     {obj,[{"id",3},
                    %%                           {"video_resource_exist",0},
                    %%                           {"format",0},
                    %%                           {"framerate",30},
                    %%                           {"video_up_bitrate",256}]}]},
                    %%                 {"privideo_recv", [
                    %%                     {obj,[{"id",1},
                    %%                           {"format",0},
                    %%                           {"framerate",25},
                    %%                           {"video_pkts_lose",555},
                    %%                           {"video_pkts_loserate",5},
                    %%                           {"video_down_bitrate",256}]}]},
                    %%                 {"assvideo_send", [
                    %%                     {obj,[{"id",1},
                    %%                           {"video_resource_exist",1},
                    %%                           {"format",0},
                    %%                           {"framerate",25},
                    %%                           {"video_up_bitrate",256}]}]},
                    %%                 {"assvideo_recv", [
                    %%                     {obj,[{"id",1},
                    %%                           {"format",0},
                    %%                           {"framerate",5},
                    %%                           {"video_pkts_lose",500},
                    %%                           {"video_pkts_loserate",5},
                    %%                           {"video_down_bitrate",256}]}]},
                    %%                 {"audio_send", [
                    %%                     {obj,[{"id",1},
                    %%                           {"format",0},
                    %%                           {"audio_up_bitrate",64}]}]},
                    %%                 {"audio_recv", [
                    %%                     {obj,[{"id",1},
                    %%                           {"format",0},
                    %%                           {"audio_pkts_lose",64},
                    %%                           {"audio_pkts_loserate",2},
                    %%                           {"audio_down_bitrate",64}]}]}]}},
                    %%       {"eventid",<<"EV_CONF_INFO">>}]}
                    lager:info("[nms_task_control] get 'EV_CONF_INFO' event!~n", []),

                    MTState = rfc4627:get_field(JsonObj, "mt_state", undefined),
                    ConfDesc = case MTState of 
                        ?IN_MULTI_CONF -> "in multi-point conference";
                        ?IN_P2P_CONF   -> "in point-to-point conference";
                        ?NOT_IN_CONF   -> "not in conference"
                    end,
                    lager:info("  -->  MTState = ~p(~p)~n", [MTState, ConfDesc]),

                    ConfInfo = rfc4627:get_field(JsonObj, "conf_info", undefined),

                    MT_E164_ = rfc4627:get_field(ConfInfo, "mt_e164", undefined),
                    lager:info("  -->  MT_E164 = ~p~n", [MT_E164_]),
                    MT_E164 = binary_to_list(MT_E164_),

                    Conf_E164_ = rfc4627:get_field(ConfInfo, "conf_e164", undefined),
                    lager:info("  -->  Conf_E164 = ~p~n", [Conf_E164_]),
                    Conf_E164 = binary_to_list(Conf_E164_),

                    ConfBitRate_ = rfc4627:get_field(ConfInfo, "bitrate", undefined),
                    lager:info("  -->  ConfBitRate = ~p~n", [ConfBitRate_]),
                    ConfBitRate = integer_to_list(ConfBitRate_),


                    %% 只针对主视频源的上行存在情况进行告警相关处理
                    PriVideoSend = rfc4627:get_field(ConfInfo, "privideo_send", undefined),
                    if
                        PriVideoSend =/= undefined ->

                            Fun0 = fun(ChannelInfo) ->
                                ChannelID = rfc4627:get_field(ChannelInfo, "id", undefined),
                                WarningCode = channelid_to_warningcode(ChannelID),
                                lager:info("[nms_task_control] WarningCode = ~p~n", [WarningCode]),
                                VideoSrcLostTriggered = case rfc4627:get_field(ChannelInfo, "video_resource_exist", undefined) of 
                                    0 -> 
                                        true;
                                    1 -> 
                                        false;
                                    _ ->
                                        lager:warning("[nms_task_control] Find no video_resource_exist, Assume =0~n", []),
                                        true
                                end,
                    %% ------------------------------------------------------------

                                update_mysql_warning_info(VideoSrcLostTriggered,MySQLTask,DevMoid,DomainMoid,
                                    ?TERMINAL,StatisticTime,EventID,WarningCode), 

                    %% ------------------------------------------------------------

                                update_redis_warning_info(VideoSrcLostTriggered,RedisTask,DevMoid,?TERMINAL,WarningCode)

                    %% ------------------------------------------------------------
                            end,

                            lists:foreach(Fun0, PriVideoSend);
                        true ->
                            lager:warning("[nms_task_control] privideo_send not exist! Weird!~n", [])
                    end,

                    %% 将会议详情信息保存到 redis 表 terminal:devid:meetingdetail 中，不区分点对点还是多点会议
                    case gen_server:call(RedisTask, {add_terminal_meeting_detail, DevMoid, MT_E164, Conf_E164, ConfBitRate}, infinity) of
                        {error, MTConfInfoErr0} ->
                            lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail' -- Failed! Error '~p'~n", 
                                [DevMoid, MTConfInfoErr0]);
                        _ ->
                            lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail' -- Success!~n", [DevMoid])
                    end,

                    %% 将会议音视频路数信息分别保存到如下 redis 表中
                    %% terminal:devid:meetingdetail:privideo_send_chan
                    %% terminal:devid:meetingdetail:privideo_recv_chan
                    %% terminal:devid:meetingdetail:assvideo_send_chan
                    %% terminal:devid:meetingdetail:assvideo_recv_chan
                    %% terminal:devid:meetingdetail:audio_send_chan
                    %% terminal:devid:meetingdetail:audio_recv_chan

                    Fun1 = fun(ChannelInfo, Acc) ->
                        ChannelID = rfc4627:get_field(ChannelInfo, "id", undefined),
                        if
                            ChannelID == undefined ->
                                Acc;
                            true ->
                                [ChannelID|Acc]
                        end
                    end,

                    if
                        PriVideoSend =/= undefined ->
                            PriVideoSendChanList = lists:reverse(lists:foldl(Fun1, [], PriVideoSend)),
                            sadd_chan_index(RedisTask, DevMoid, privideo_send_chan, PriVideoSendChanList);
                        true -> void
                    end,

                    PriVideoRecv = rfc4627:get_field(ConfInfo, "privideo_recv", undefined),
                    if
                        PriVideoRecv =/= undefined ->
                            PriVideoRecvChanList = lists:reverse(lists:foldl(Fun1, [], PriVideoRecv)),
                            sadd_chan_index(RedisTask, DevMoid, privideo_recv_chan, PriVideoRecvChanList);
                        true -> void
                    end,

                    AssVideoSend = rfc4627:get_field(ConfInfo, "assvideo_send", undefined),
                    if
                        AssVideoSend =/= undefined ->
                            AssVideoSendChanList = lists:reverse(lists:foldl(Fun1, [], AssVideoSend)),
                            sadd_chan_index(RedisTask, DevMoid, assvideo_send_chan, AssVideoSendChanList);
                        true -> void
                    end,

                    AssVideoRecv = rfc4627:get_field(ConfInfo, "assvideo_recv", undefined),
                    if
                        AssVideoRecv =/= undefined ->
                            AssVideoRecvChanList = lists:reverse(lists:foldl(Fun1, [], AssVideoRecv)),
                            sadd_chan_index(RedisTask, DevMoid, assvideo_recv_chan, AssVideoRecvChanList);
                        true -> void
                    end,

                    AudioSend = rfc4627:get_field(ConfInfo, "audio_send", undefined),
                    if
                        AudioSend =/= undefined ->
                            AudioSendChanList = lists:reverse(lists:foldl(Fun1, [], AudioSend)),
                            sadd_chan_index(RedisTask, DevMoid, audio_send_chan, AudioSendChanList);
                        true -> void
                    end,

                    AudioRecv = rfc4627:get_field(ConfInfo, "audio_recv", undefined),
                    if
                        AudioRecv =/= undefined ->
                            AudioRecvChanList = lists:reverse(lists:foldl(Fun1, [], AudioRecv)),
                            sadd_chan_index(RedisTask, DevMoid, audio_recv_chan, AudioRecvChanList);
                        true -> void
                    end,


                    %% privideo_send 详细信息保存
                    Fun_pvs = fun(ChannelInfo) ->

                        %% common
                        ChanID_ = rfc4627:get_field(ChannelInfo, "id", undefined),
                        ChanID = integer_to_list(ChanID_),
                        Format    = rfc4627:get_field(ChannelInfo, "format", undefined),

                        %% privideo_send
                        VideoSrcLost     = rfc4627:get_field(ChannelInfo, "video_resource_exist", undefined),
                        Framerate        = rfc4627:get_field(ChannelInfo, "framerate", undefined),
                        VideoUpBitrate   = rfc4627:get_field(ChannelInfo, "video_up_bitrate", undefined),

                        KeyValuePairs = [
                            "id",                     ChanID,
                            "format",                 video_format_trans(Format),
                            "video_resource_exist",   integer_to_list(VideoSrcLost),
                            "framerate",              integer_to_list(Framerate),
                            "video_up_bitrate",       integer_to_list(VideoUpBitrate)
                        ],

                        %% 将主视频发送通道信息保存到 redis 表 terminal:devid:meetingdetail:privideo_send_chan:N 中
                        case gen_server:call(RedisTask, 
                                {add_terminal_channel_detail, DevMoid, privideo_send_chan, ChanID, KeyValuePairs}, infinity) of
                            {error, MTConfInfoErr1} ->
                                lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail:privideo_send_chan:~p' -- Failed! 
                                    Error '~p'~n", [DevMoid, ChanID, MTConfInfoErr1]);
                            _ ->
                                lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail:privideo_send_chan:~p' -- Success!~n", 
                                    [DevMoid, ChanID])
                        end
                    end,
                    lists:foreach(Fun_pvs, PriVideoSend),


                    %% privideo_recv 详细信息保存
                    Fun_pvr = fun(ChannelInfo) ->

                        %% common
                        ChanID_ = rfc4627:get_field(ChannelInfo, "id", undefined),
                        ChanID = integer_to_list(ChanID_),
                        Format    = rfc4627:get_field(ChannelInfo, "format", undefined),

                        %% privideo_recv
                        VideoPktLost     = rfc4627:get_field(ChannelInfo, "video_pkts_lose", undefined),
                        VideoPktLostRate = rfc4627:get_field(ChannelInfo, "video_pkts_loserate", undefined),
                        VideoDownBitrate = rfc4627:get_field(ChannelInfo, "video_down_bitrate", undefined),

                        KeyValuePairs = [
                            "id",                     ChanID,
                            "format",                 video_format_trans(Format),
                            "video_pkts_lose",        integer_to_list(VideoPktLost),
                            "video_pkts_loserate",    integer_to_list(VideoPktLostRate),
                            "video_down_bitrate",     integer_to_list(VideoDownBitrate)
                        ],

                        %% 将主视频接收通道信息保存到 redis 表 terminal:devid:meetingdetail:privideo_recv_chan:N 中
                        case gen_server:call(RedisTask, 
                                {add_terminal_channel_detail, DevMoid, privideo_recv_chan, ChanID, KeyValuePairs}, infinity) of
                            {error, MTConfInfoErr2} ->
                                lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail:privideo_recv_chan:~p' -- Failed! 
                                    Error '~p'~n", [DevMoid, ChanID, MTConfInfoErr2]);
                            _ ->
                                lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail:privideo_recv_chan:~p' -- Success!~n", 
                                    [DevMoid, ChanID])
                        end
                    end,
                    lists:foreach(Fun_pvr, PriVideoRecv),


                    %% assvideo_send 详细信息保存
                    Fun_avs = fun(ChannelInfo) ->

                        %% common
                        ChanID_ = rfc4627:get_field(ChannelInfo, "id", undefined),
                        ChanID = integer_to_list(ChanID_),
                        Format    = rfc4627:get_field(ChannelInfo, "format", undefined),

                        %% assvideo_send
                        VideoSrcLost     = rfc4627:get_field(ChannelInfo, "video_resource_exist", undefined),
                        Framerate        = rfc4627:get_field(ChannelInfo, "framerate", undefined),
                        VideoUpBitrate   = rfc4627:get_field(ChannelInfo, "video_up_bitrate", undefined),

                        KeyValuePairs = [
                            "id",                     ChanID,
                            "format",                 video_format_trans(Format),
                            "video_resource_exist",   integer_to_list(VideoSrcLost),
                            "framerate",              integer_to_list(Framerate),
                            "video_up_bitrate",       integer_to_list(VideoUpBitrate)
                        ],

                        %% 将辅视频发送通道信息保存到 redis 表 terminal:devid:meetingdetail:assvideo_send_chan:N 中
                        case gen_server:call(RedisTask, 
                                {add_terminal_channel_detail, DevMoid, assvideo_send_chan, ChanID, KeyValuePairs}, infinity) of
                            {error, MTConfInfoErr3} ->
                                lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail:assvideo_send_chan:~p' -- Failed! 
                                    Error '~p'~n", [DevMoid, ChanID, MTConfInfoErr3]);
                            _ ->
                                lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail:assvideo_send_chan:~p' -- Success!~n", 
                                    [DevMoid, ChanID])
                        end
                    end,
                    lists:foreach(Fun_avs, AssVideoSend),


                    %% assvideo_recv 详细信息保存
                    Fun_avr = fun(ChannelInfo) ->

                        %% common
                        ChanID_ = rfc4627:get_field(ChannelInfo, "id", undefined),
                        ChanID = integer_to_list(ChanID_),
                        Format    = rfc4627:get_field(ChannelInfo, "format", undefined),

                        %% assvideo_recv
                        VideoPktLost     = rfc4627:get_field(ChannelInfo, "video_pkts_lose", undefined),
                        VideoPktLostRate = rfc4627:get_field(ChannelInfo, "video_pkts_loserate", undefined),
                        VideoDownBitrate = rfc4627:get_field(ChannelInfo, "video_down_bitrate", undefined),

                        KeyValuePairs = [
                            "id",                     ChanID,
                            "format",                 video_format_trans(Format),
                            "video_pkts_lose",        integer_to_list(VideoPktLost),
                            "video_pkts_loserate",    integer_to_list(VideoPktLostRate),
                            "video_down_bitrate",     integer_to_list(VideoDownBitrate)
                        ],

                        %% 将辅视频接收通道信息保存到 redis 表 terminal:devid:meetingdetail:assvideo_recv_chan:N 中
                        case gen_server:call(RedisTask, 
                                {add_terminal_channel_detail, DevMoid, assvideo_recv_chan, ChanID, KeyValuePairs}, infinity) of
                            {error, MTConfInfoErr4} ->
                                lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail:assvideo_recv_chan:~p' -- Failed! 
                                    Error '~p'~n", [DevMoid, ChanID, MTConfInfoErr4]);
                            _ ->
                                lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail:assvideo_recv_chan:~p' -- Success!~n", 
                                    [DevMoid, ChanID])
                        end
                    end,
                    lists:foreach(Fun_avr, AssVideoRecv),


                    %% audio_send 详细信息保存
                    Fun_as = fun(ChannelInfo) ->

                        %% common
                        ChanID_ = rfc4627:get_field(ChannelInfo, "id", undefined),
                        ChanID = integer_to_list(ChanID_),
                        Format    = rfc4627:get_field(ChannelInfo, "format", undefined),

                        %% audio_send
                        AudioUpBitrate   = rfc4627:get_field(ChannelInfo, "audio_up_bitrate", undefined),

                        KeyValuePairs = [
                            "id",                     ChanID,
                            "format",                 audio_format_trans(Format),
                            "audio_up_bitrate",       integer_to_list(AudioUpBitrate)
                        ],

                        %% 将音频发送通道信息保存到 redis 表 terminal:devid:meetingdetail:audio_send_chan:N 中
                        case gen_server:call(RedisTask, 
                                {add_terminal_channel_detail, DevMoid, audio_send_chan, ChanID, KeyValuePairs}, infinity) of
                            {error, MTConfInfoErr5} ->
                                lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail:audio_send_chan:~p' -- Failed! 
                                    Error '~p'~n", [DevMoid, ChanID, MTConfInfoErr5]);
                            _ ->
                                lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail:audio_send_chan:~p' -- Success!~n", 
                                    [DevMoid, ChanID])
                        end
                    end,
                    lists:foreach(Fun_as, AudioSend),


                    %% audio_recv 详细信息保存
                    Fun_ar = fun(ChannelInfo) ->

                        %% common
                        ChanID_ = rfc4627:get_field(ChannelInfo, "id", undefined),
                        ChanID = integer_to_list(ChanID_),
                        Format    = rfc4627:get_field(ChannelInfo, "format", undefined),

                        %% audio_recv
                        AudioPktLost     = rfc4627:get_field(ChannelInfo, "audio_pkts_lose", undefined),
                        AudioPktLoseRate = rfc4627:get_field(ChannelInfo, "audio_pkts_loserate", undefined),
                        AudioDownBitrate = rfc4627:get_field(ChannelInfo, "audio_down_bitrate", undefined),

                        KeyValuePairs = [
                            "id",                     ChanID,
                            "format",                 audio_format_trans(Format),
                            "audio_pkts_lose",        integer_to_list(AudioPktLost),
                            "audio_pkts_loserate",    integer_to_list(AudioPktLoseRate),
                            "audio_down_bitrate",     integer_to_list(AudioDownBitrate)
                        ],

                        %% 将音频发送通道信息保存到 redis 表 terminal:devid:meetingdetail:audio_recv_chan:N 中
                        case gen_server:call(RedisTask, 
                                {add_terminal_channel_detail, DevMoid, audio_recv_chan, ChanID, KeyValuePairs}, infinity) of
                            {error, MTConfInfoErr6} ->
                                lager:warning("[nms_task_control] 'HMSET terminal:~p:meetingdetail:audio_recv_chan:~p' -- Failed! 
                                    Error '~p'~n", [DevMoid, ChanID, MTConfInfoErr6]);
                            _ ->
                                lager:info("[nms_task_control] 'HMSET terminal:~p:meetingdetail:audio_recv_chan:~p' -- Success!~n", 
                                    [DevMoid, ChanID])
                        end
                    end,                    
                    lists:foreach(Fun_ar, AudioRecv),

                    io:format("", []);

                OtherEvent     ->
                    lager:info("[nms_task_control] get '~p' event, do nothing!~n", [OtherEvent])
            end;

        {error,<<"Key Error">>} ->
            lager:warning("[nms_task_control] get <<\"Key Error\">> for key ~p~n", [DevMoid]),
            throw(redis_key_error);
        {error, no_connection} ->
            lager:error("[nms_task_control] lost redis connection!"),
            throw(redis_connection_lost)
    end.

collector_heartbeat_proc(JsonObj, RedisTask, _MySQLTask) ->

    %% rpttime 格式     year-month-day/hour:min:sec
    %% rpttime 格式变更 year/month/day:hour:min:sec
    StatisticTime_ = rfc4627:get_field(JsonObj, "rpttime", undefined),
    case StatisticTime_ of
        undefined ->
            {{Year,Month,Day},{Hour,Min,Sec}} = calendar:now_to_local_time(os:timestamp()),
            StatisticTime = integer_to_list(Year)++"/"++integer_to_list(Month)++"/"++integer_to_list(Day)++":"++
                            integer_to_list(Hour)++":"++integer_to_list(Min)++":"++integer_to_list(Sec),
            lager:info("[nms_task_control] find no rpttime, so make it myself:~p~n", 
                [StatisticTime]);
        _ ->
            StatisticTime = binary_to_list(StatisticTime_)
    end,
    lager:info("  -->  StatisticTime = ~p~n", [StatisticTime]),

    EventID = rfc4627:get_field(JsonObj, "eventid", undefined),
    lager:info("  -->  EventID = ~p~n", [EventID]),

    %% 通过 "eventid" 判定为信息类型
    case EventID of

        <<"EV_COLLECTOR_HEARTBEAT">>     ->
            %% 消息举例
            %% {obj,[{"devtype",<<"COLLECTOR">>},
            %%       {"rpttime",<<"2015/1/14:14:6:40">>},
            %%       {"collectorid",<<"60a44c502a60">>},
            %%       {"eventid",<<"EV_COLLECTOR_HEARTBEAT">>}]}

            lager:info("[nms_task_control] get 'EV_COLLECTOR_HEARTBEAT' event!~n", []),

            CollectorID_ = rfc4627:get_field(JsonObj, "collectorid", undefined),
            lager:info("  -->  CollectorID = ~p~n", [CollectorID_]),
            CollectorID = binary_to_list(CollectorID_),

            update_timer_by_collectorid(RedisTask, CollectorID),

            %% 向 Redis 表 collector 中保存 collectorid
            %% 此操作存在冗余，后续待优化
            case gen_server:call(RedisTask, {add_collectorid, CollectorID}, infinity) of
                {error, CollectorHBErr0} ->
                    lager:warning("[nms_task_control] 'SADD collector ~p' -- Failed! Error '~p'~n", 
                        [CollectorID, CollectorHBErr0]);
                {ok, _} ->
                    lager:info("[nms_task_control] 'SADD collector ~p' -- Success!~n", [CollectorID])
            end,            

            io:format("", []);

        OtherEvent     ->
            lager:info("[nms_task_control] get '~p' event, do nothing!~n", [OtherEvent])
    end.


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
        {logical, Type}  ->
            lager:info("Devtype => {logical, ~p}~n", [Type]),
            lager:notice("<=============== LOGICAL DEVICE ===============>"),
            logical_device_proc(JsonObj, RedisTask, MySQLTask, Type),
            lager:notice("<=============== LOGICAL DEVICE ===============>"),
            io:format("", []);
        {terminal, Type} ->
            lager:info("Devtype => {terminal, ~p}~n", [Type]),
            lager:notice("<=============== TERMINAL DEVICE ===============>"),
            terminal_device_proc(JsonObj, RedisTask, MySQLTask, Type),
            lager:notice("<=============== TERMINAL DEVICE ===============>"),
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
            io:format("Result=~p~n~nRemainder=~p~n~n", [Result, Remainder]),
            lager:info("~n~nResult=~p~n~nRemainder=~p~n~n", [Result, Remainder]),
            lager:notice("#####  rfc4627:decode  #####"),
            msg_parser(Result, State);

        {error, Error} ->
            lager:error("after rfc4627:decode   --> ~nerror=[~p]~n", [Error])
    end,
    {noreply, State};

handle_info({timeout, TimerRef, {delete_collector_info, CollectorID}=Msg}, #state{redis_task=RedisTask} = State) ->
    lager:notice("######  Recv {timeout, ~p, ~p}", [TimerRef, Msg]),

    %% 删除 Redis 的 SET 表 collector:collectorid:online
    case gen_server:call(RedisTask, {del_collector_online_device_all, CollectorID}, infinity) of
        {error, TimeOutErr0} ->
            lager:warning("[nms_task_control] 'DEL collector:~p:online' -- Failed! Error '~p'~n", 
                [CollectorID, TimeOutErr0]);
        {ok, _} ->
            lager:info("[nms_task_control] 'DEL collector:~p:online' -- Success!~n", [CollectorID])
    end,

    %% 删除 Redis 的 HASH 表 collector:collectorid:timer
    case gen_server:call(RedisTask, {del_heartbeat_timer_by_collectorid, CollectorID}, infinity) of
        {error, TimeOutErr1} ->
            lager:warning("[nms_task_control] 'DEL collector:~p:timer' -- Failed! Error '~p'~n", 
                [CollectorID, TimeOutErr1]);
        {ok, _} ->
            lager:info("[nms_task_control] 'DEL collector:~p:timer' -- Success!~n", [CollectorID])
    end,

    %% 删除 Redis 的 SET 表 collector 中的 collectorid
    case gen_server:call(RedisTask, {del_collectorid, CollectorID}, infinity) of
        {error, TimeOutErr2} ->
            lager:warning("[nms_task_control] 'SREM collector ~p' -- Failed! Error '~p'~n", 
                [CollectorID, TimeOutErr2]);
        {ok, _} ->
            lager:info("[nms_task_control] 'SREM collector ~p' -- Success!~n", [CollectorID])
    end,

    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
