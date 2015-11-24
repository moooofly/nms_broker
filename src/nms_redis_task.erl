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

-module(nms_redis_task).
-behaviour(gen_server).

-export([init/1, 
         terminate/2, 
         code_change/3, 
         handle_call/3, 
         handle_cast/2,
         handle_info/2]).

-export([start_link/2]).

-record(redis_params, {
            host           = "127.0.0.1",
            port           = 6379,
            database       = 0,
            password       = "",
            reconnectsleep = 100
    }).

-record(state, {
            tref          = undefined :: undefined | nms_api:ref(),
            redis_params  = #redis_params{},
            redis_con     = undefined :: undefined | pid()
    }).


%% Args 为包含 host、port、database、password、reconnect_sleep 的元组列表
%% 如 [{host,"127.0.0.1"},{port,6379},{database,0},{password,""},{reconnect_sleep,100}]
start_link(TRef, Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [TRef, Args], []);
start_link(_, _) ->
    lager:error("[RedisTask] Args must be proplists!Error!"),
    {error, args_not_list}.

init([TRef, Args]) ->
    process_flag(trap_exit, true),

    Host           = proplists:get_value(host, Args, "127.0.0.1"),
    Port           = proplists:get_value(port, Args, 6379),
    Database       = proplists:get_value(database, Args, 0),
    Password       = proplists:get_value(password, Args, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep, Args, 100),

    RedisParams = #redis_params{host=Host, port=Port, database=Database, 
                                password=Password, reconnectsleep=ReconnectSleep},
    State = #state{tref=TRef, redis_params=RedisParams},

    case eredis:start_link(Host, Port, Database, Password, ReconnectSleep) of
        {ok, RedisCon} ->
            lager:info("[RedisTask] init => create Connection Success! Pid = ~p~n", [RedisCon]),
            nms_config:set_redis_task(TRef, self()),
            {ok, State#state{redis_con=RedisCon}};
        {error, ConnErr} ->
            %% ConnErr -> {connection_error,{connection_error,econnrefused}}
            lager:error("[RedisTask] create Connection Failed! Error '~p'~n", [ConnErr]),
            {stop, {create_connection_error, ConnErr}}
    end.


handle_call( {get_service_by_platform, PlatformDomainMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = domain_handler:get_service_by_platform(RedisCon,PlatformDomainMoid),
            {reply, Result, State}
    end;

handle_call( {del_xmpp_in_all_domains, XmppDomainKey}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:del_xmpp_in_all_domains(RedisCon,XmppDomainKey),
            {reply, Result, State}
    end;

handle_call( {add_xmpp_in_all_domains, XmppDomainKey}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_xmpp_in_all_domains(RedisCon,XmppDomainKey),
            {reply, Result, State}
    end;

handle_call( {add_xmpp_online_statistic, DomainMoid, OnlineNum}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_xmpp_online_statistic(RedisCon,DomainMoid,OnlineNum),
            {reply, Result, State}
    end;

handle_call( {del_meeting_terminal, ConfE164, TerminalE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:del_meeting_terminal(RedisCon,ConfE164,TerminalE164),
            {reply, Result, State}
    end;

handle_call( {add_meeting_terminal, ConfE164, TerminalE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:add_meeting_terminal(RedisCon,ConfE164,TerminalE164),
            {reply, Result, State}
    end;

handle_call( {add_terminal_leave_meeting_info, TerminalMoid, ConfE164, TerEnterTimes, TerLeaveTime, TerLeaveReason}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:add_terminal_leave_meeting_info(RedisCon,TerminalMoid,ConfE164,TerEnterTimes,
                TerLeaveTime,TerLeaveReason),
            {reply, Result, State}
    end;

handle_call( {add_terminal_enter_meeting_info, TerminalMoid, ConfE164, TerEnterTimes, TerEnterTime}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:add_terminal_enter_meeting_info(RedisCon,TerminalMoid,ConfE164,TerEnterTimes,TerEnterTime),
            {reply, Result, State}
    end;

handle_call( {get_terminal_enter_meeting_times, TerminalMoid, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:get_terminal_enter_meeting_times(RedisCon,TerminalMoid,ConfE164),
            {reply, Result, State}
    end;

handle_call( {inc_terminal_enter_meeting_times, TerminalMoid, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:inc_terminal_enter_meeting_times(RedisCon,TerminalMoid,ConfE164),
            {reply, Result, State}
    end;

handle_call( {dec_port_meeting_terminal_num, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:dec_port_meeting_terminal_num(RedisCon,ConfE164),
            {reply, Result, State}
    end;

handle_call( {inc_port_meeting_terminal_num, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:inc_port_meeting_terminal_num(RedisCon,ConfE164),
            {reply, Result, State}
    end;

handle_call( {dec_traditional_meeting_terminal_num, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:dec_traditional_meeting_terminal_num(RedisCon,ConfE164),
            {reply, Result, State}
    end;

handle_call( {inc_traditional_meeting_terminal_num, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:inc_traditional_meeting_terminal_num(RedisCon,ConfE164),
            {reply, Result, State}
    end;

handle_call( {del_port_meeting, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:del_port_meeting(RedisCon,ConfE164),
            {reply, Result, State}
    end;

handle_call( {add_port_meeting, DomainMoid, MeetingID, Name, BandWidth, Terminal, Port, StartTime, StopTime}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:add_port_meeting(RedisCon,DomainMoid,MeetingID,Name,BandWidth,Terminal,Port,StartTime,StopTime),
            {reply, Result, State}
    end;

handle_call( {del_traditional_meeting, ConfE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:del_traditional_meeting(RedisCon,ConfE164),
            {reply, Result, State}
    end;

handle_call( {add_traditional_meeting, DomainMoid, MeetingID, Name, BandWidth, Device, StartTime, StopTime}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:add_traditional_meeting(RedisCon,DomainMoid,MeetingID,Name,BandWidth,
                Device,StartTime,StopTime),
            {reply, Result, State}
    end;

handle_call( {del_p2p_meeting, CallerE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:del_p2p_meeting(RedisCon,CallerE164),
            {reply, Result, State}
    end;

handle_call( {add_p2p_meeting, CallerDomainMoid, CallerE164, CallerName, CallerType, 
        CalleeDomainMoid, CalleeE164, CalleeName, CalleeType, Bandwidth, StartTime}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = meeting_handler:add_p2p_meeting(RedisCon,CallerDomainMoid,CallerE164,CallerName,CallerType,
                CalleeDomainMoid,CalleeE164,CalleeName,CalleeType,Bandwidth,StartTime),
            {reply, Result, State}
    end;

handle_call( {del_pas_in_all_domains, CompletePasDomainInfo}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:del_pas_in_all_domains(RedisCon,CompletePasDomainInfo),
            {reply, Result, State}
    end;

handle_call( {add_pas_in_all_domains, CompletePasDomainInfo}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_pas_in_all_domains(RedisCon,CompletePasDomainInfo),
            {reply, Result, State}
    end;

handle_call( {add_pas_online_statistic, DomainMoid, DevMoid, H323Online, SIPOnline, MonitorOnline}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_pas_online_statistic(RedisCon,DomainMoid,DevMoid,H323Online,SIPOnline,MonitorOnline),
            {reply, Result, State}
    end;

handle_call( {add_pas_moid, DomainMoid, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_pas_moid(RedisCon,DomainMoid,DevMoid),
            {reply, Result, State}
    end;

handle_call( {add_physical_ip, DevMoid, PhySerIPString}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:add_physical_ip(RedisCon,DevMoid,PhySerIPString),
            {reply, Result, State}
    end;

handle_call( {add_terminal_channel_detail, DevMoid, ChannelType, ChanID, InfoList}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_channel_detail(RedisCon,DevMoid,ChannelType,ChanID,InfoList),
            {reply, Result, State}
    end;

handle_call( {add_terminal_meeting_channel, DevMoid, ChannelType, ChanList}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_meeting_channel(RedisCon,DevMoid,ChannelType,ChanList),
            {reply, Result, State}
    end;

handle_call( {del_terminal_meeting_channels, TerminalMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_meeting_channels(RedisCon,TerminalMoid),
            {reply, Result, State}
    end;

handle_call( {del_terminal_meeting_detail, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_meeting_detail(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {add_terminal_meeting_detail, DevMoid, MT_E164, Conf_E164, ConfBitRate}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_meeting_detail(RedisCon,DevMoid,MT_E164,Conf_E164,ConfBitRate),
            {reply, Result, State}
    end;

handle_call( {del_terminal_warning, DevMoid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_warning(RedisCon,DevMoid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {add_terminal_warning, DevMoid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_warning(RedisCon,DevMoid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {update_terminal_connections, DevMoid, ServerConInfoList}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = [{ok, return_value()} | {error, Reason::binary()}] | {error, no_connection}
            Result = terminal_handler:update_terminal_connections(RedisCon,DevMoid,ServerConInfoList),
            {reply, Result, State}
    end;

handle_call( {add_terminal_connections, DevMoid, ConSrvTypeInfo}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = [{ok, return_value()} | {error, Reason::binary()}] | {error, no_connection}
            Result = terminal_handler:add_terminal_connections(RedisCon,DevMoid,ConSrvTypeInfo),
            {reply, Result, State}
    end;

handle_call( {get_terminal_mem_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
            Result = system_set_handler:get_terminal_mem_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_terminal_cpu_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            %% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
            Result = system_set_handler:get_terminal_cpu_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {add_terminal_resource, DevMoid, Cpu, Disk, Memory}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_resource(RedisCon,DevMoid,Cpu,Disk,Memory),
            {reply, Result, State}
    end;

handle_call( {add_terminal_bandwidth_info, DevMoid, SendBandWidth, SendDropRate, RecvBandWidth, RecvDropRate}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_bandwidth_info(RedisCon,DevMoid,
                SendBandWidth,SendDropRate,RecvBandWidth,RecvDropRate),
            {reply, Result, State}
    end;

handle_call( {add_terminal_net_info, DevMoid, TerIp, TerNatIp, TerDns}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_net_info(RedisCon,DevMoid,TerIp,TerNatIp,TerDns),
            {reply, Result, State}
    end;

handle_call( {add_terminal_aps_and_net_info, DevMoid, TerIp, TerNatIp, TerDns, APSDomainName, ApsIP}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_aps_and_net_info(RedisCon,DevMoid,TerIp,TerNatIp,TerDns,APSDomainName,ApsIP),
            {reply, Result, State}
    end;

handle_call( {add_terminal_running_info, DevMoid, Type, Version, OS, CpuType, CpuFreq, CpuNum, Memory}, 
        _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_running_info(RedisCon,DevMoid,Type,Version,OS,CpuType,CpuFreq,CpuNum,Memory),
            {reply, Result, State}
    end;

handle_call( {del_terminal_running_info, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_running_info(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {del_terminal_warning_all, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_warning_all(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {del_terminal_resource, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_resource(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {del_terminal_connections, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:del_terminal_connections(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {del_terminal_online, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_online(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {add_terminal_online, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:add_terminal_online(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {get_terminal_base_info, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:get_terminal_base_info(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {get_terminal_base_info_by_e164, DevE164}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = terminal_handler:get_terminal_base_info_by_e164(RedisCon,DevE164),
            {reply, Result, State}
    end;

handle_call( {get_logic_server_info_by_guid, DevGuid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:get_logic_server_info_by_guid(RedisCon,DevGuid),
            {reply, Result, State}
    end;

handle_call( {del_logic_server_warning_all, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:del_logic_server_warning_all(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {del_logic_server_connections, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:del_logic_server_connections(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {del_logic_server_online, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:del_logic_server_online(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {add_logic_server_online, DevMoid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_logic_server_online(RedisCon,DevMoid),
            {reply, Result, State}
    end;

handle_call( {add_collectorid, CollectorID}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:add_collectorid(RedisCon,CollectorID),
            {reply, Result, State}
    end;

handle_call( {del_collectorid, CollectorID}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_collectorid(RedisCon,CollectorID),
            {reply, Result, State}
    end;

handle_call( {set_heartbeat_timer_by_collectorid, CollectorID, TimerRef}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:set_heartbeat_timer_by_collectorid(RedisCon,CollectorID,TimerRef),
            {reply, Result, State}
    end;

handle_call( {get_heartbeat_timer_by_collectorid, CollectorID}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:get_heartbeat_timer_by_collectorid(RedisCon,CollectorID),
            {reply, Result, State}
    end;

handle_call( {del_heartbeat_timer_by_collectorid, CollectorID}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_heartbeat_timer_by_collectorid(RedisCon,CollectorID),
            {reply, Result, State}
    end;

handle_call( {add_collector_online_device, CollectorID, DevGuid, DevType}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:add_collector_online_device(RedisCon,CollectorID,DevGuid,DevType),
            {reply, Result, State}
    end;

handle_call( {del_collector_online_device, CollectorID, DevGuid, DevType}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_collector_online_device(RedisCon,CollectorID,DevGuid,DevType),
            {reply, Result, State}
    end;

handle_call( {del_collector_online_device_all, CollectorID}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_collector_online_device_all(RedisCon,CollectorID),
            {reply, Result, State}
    end;

handle_call( {del_physical_server_warning_all, DevGuid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_physical_server_warning_all(RedisCon,DevGuid),
            {reply, Result, State}
    end;

handle_call( {del_physical_server_resource, DevGuid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_physical_server_resource(RedisCon,DevGuid),
            {reply, Result, State}
    end;

handle_call( {del_physical_server_online, DevGuid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_physical_server_online(RedisCon,DevGuid),
            {reply, Result, State}
    end;

handle_call( {add_physical_server_online, DevGuid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:add_physical_server_online(RedisCon,DevGuid),
            {reply, Result, State}
    end;

handle_call( {del_logical_server_warning, DevGuid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:del_logical_server_warning(RedisCon,DevGuid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {add_logical_server_warning, DevGuid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = logical_server_handler:add_logical_server_warning(RedisCon,DevGuid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {del_physical_server_warning, DevGuid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:del_physical_server_warning(RedisCon,DevGuid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {add_physical_server_warning, DevGuid, WarningCode}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:add_physical_server_warning(RedisCon,DevGuid,WarningCode),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_net_resource, DevGuid, PortIn, PortOut}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_net_resource(RedisCon,DevGuid,PortIn,PortOut),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_mem_resource, DevGuid, Mem}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_mem_resource(RedisCon,DevGuid,Mem),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_disk_resource, DevGuid, Disk}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_disk_resource(RedisCon,DevGuid,Disk),
            {reply, Result, State}
    end;

handle_call( {update_physical_server_cpu_resource, DevGuid, Cpu}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:update_physical_server_cpu_resource(RedisCon,DevGuid,Cpu),
            {reply, Result, State}
    end;

handle_call( {get_physical_server_info_by_guid, DevGuid}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = physical_server_handler:get_physical_server_info_by_guid(RedisCon,DevGuid),
            {reply, Result, State}
    end;

handle_call( {get_server_net_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = system_set_handler:get_server_net_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_server_disk_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = system_set_handler:get_server_disk_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_server_mem_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = system_set_handler:get_server_mem_limit_redis(RedisCon),
            {reply, Result, State}
    end;

handle_call( {get_server_cpu_limit}, _From, #state{redis_con=RedisCon}=State ) ->
    case RedisCon of 
        undefined ->
            {reply, {error, no_connection}, State};
        _ ->
            Result = system_set_handler:get_server_cpu_limit_redis(RedisCon),
            {reply, Result, State}
    end;
    
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(reconnect, #state{tref=TRef, redis_params=RedisParams}=State) ->

    #redis_params{host=Host,port=Port,database=Database, 
                          password=Password,reconnectsleep=ReconnectSleep} = RedisParams,

    case eredis:start_link(Host, Port, Database, Password, ReconnectSleep) of
        {ok, NewRedisCon} ->
            lager:info("[RedisTask] re-Create Connection Success! Pid = ~p~n", [NewRedisCon]),
            nms_config:set_redis_task(TRef, self()),
            {noreply, State#state{redis_con=NewRedisCon}};
        {error, ConnErr} ->
            lager:warning("[RedisTask] re-Create Connection Failed! Error '~p'~n", [ConnErr]),
            erlang:send_after(5000, self(), reconnect),
            {noreply, State}
    end;

handle_info({'EXIT', Pid, Info}, 
        #state{tref=TRef,redis_con=OldRedisCon,redis_params=RedisParams}=State) ->

    #redis_params{host=Host,port=Port,database=Database, 
                          password=Password,reconnectsleep=ReconnectSleep} = RedisParams,
    case Pid of 
        OldRedisCon ->
            lager:info("[RedisTask] recv {'EXIT', ~p, ~p}, Connection down!~n", [Pid, Info]),

            case eredis:start_link(Host, Port, Database, Password, ReconnectSleep) of
                {ok, NewRedisCon} ->
                    lager:info("[RedisTask] re-Create Connection Success! Pid = ~p~n", [NewRedisCon]),
                    nms_config:set_redis_task(TRef, self()),
                    {noreply, State#state{redis_con=NewRedisCon}};
                {error, ConnErr} ->
                    lager:warning("[RedisTask] re-Create Connection Failed! Error '~p'~n", [ConnErr]),
                    erlang:send_after(5000, self(), reconnect),
                    {noreply, State}
            end;
        _          ->
            lager:notice("[RedisTask] recv {'EXIT', ~p, ~p}, something down!~n", [Pid, Info]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    lager:info("[RedisTask] handle_info => Info(~p)~n", [Info]),
    {noreply, State}.

terminate(Reason, #state{tref=TRef}=_State) ->
    lager:warning("[RedisTask] terminate => Reason(~p)~n", [Reason]),
    TaskConPid = nms_config:get_task_control(TRef),
    exit(TaskConPid, {restart, from_redis_task}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.