-module(terminal_handler).

-export([add_terminal/5,
         del_terminal/3,

         add_terminal_channel_detail/5,       %% add
         add_terminal_meeting_channel/4,      %% add
         del_terminal_meeting_channels/2,     %% add

         add_terminal_meeting_detail/5,       %% add
         del_terminal_meeting_detail/2,       %% add

         add_terminal_running_info/9,
         del_terminal_running_info/2,

         add_terminal_bandwidth_info/6,       %% add
         add_terminal_aps_and_net_info/7,     %% add
         add_terminal_net_info/5,             %% update
         del_terminal_net_info/2,

         add_terminal_resource/5,
         del_terminal_resource/2,

         del_terminal_warning_all/2,          %% add
         add_terminal_warning/3,
         del_terminal_warning/3,

         add_terminal_online/2,
         del_terminal_online/2,

         add_terminal_connections/3,
         del_terminal_connections/2,
         add_terminal_connection/3,
         del_terminal_connection/3,
         update_terminal_connections/3,       %% update
         update_terminal_connection/4,

         get_all_terminal/2,
         get_terminal_count/2,
         get_terminal_base_info/2,            %% update
         get_terminal_base_info_by_e164/2,    %% update
         get_terminal_running_info/2,
         get_terminal_net_info/2,
         get_terminal_resource/2,
         get_terminal_online/2,
         get_terminal_warning/2,
         get_terminal_connections/2]).
		
%% 添加一个终端设备(散列类型数据)
%% 添加终端设备的同时把终端设备的moid添加到终端设备所属的用户域下
%% 返回值 : {ok,success}
-spec add_terminal(pid(),string(),string(),string(),string()) -> {ok,success}.
add_terminal( RedisClient,DevMoid,DomainMoid,Name,E164 ) ->
	KeyTerminal = format_key_terminal(DomainMoid),
	
	%% 以moid作为key
	KeyMoidInfo = format_key_base_info(DevMoid),
	KeyMoidInfoPairs = [KeyMoidInfo,"moid",DevMoid,"domain_moid",DomainMoid,"name",Name,"e164",E164],
	
	%% 以e164号码作为key
	KeyE164Info = format_key_base_info_by_e164(E164),
	KeyE164InfoPairs = [KeyE164Info,"moid",DevMoid,"domain_moid",DomainMoid,"name",Name,"e164",E164],
	
	eredis:q(RedisClient,["MULTI"]),
	eredis:q(RedisClient,["SADD",KeyTerminal,DevMoid]),
	eredis:q(RedisClient,["HMSET"|KeyMoidInfoPairs]),
	eredis:q(RedisClient,["HMSET"|KeyE164InfoPairs]),
	eredis:q(RedisClient,["EXEC"]),
	
	{ok,success}.
	
%% 删除一个终端设备(散列类型数据)
%% 删除终端设备的同时把终端设备的moid从终端设备所属的用户域下删除
%% 返回值 : {ok,success}
-spec del_terminal(pid(),string(),string()) -> {ok,success}.	
del_terminal( RedisClient,DevMoid,DomainMoid ) ->
	Key = format_key_terminal( DomainMoid ),
	KeyMoidInfo = format_key_base_info( DevMoid ),
	KeyRunningInfo = format_key_running_info( DevMoid ),
	
	case get_terminal_base_info(RedisClient,DevMoid) of
		{ok,{_,_,_,E164}} ->
			KeyE64Info = format_key_base_info_by_e164( E164 ),
	
			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",Key,DevMoid]),
			eredis:q(RedisClient,["DEL",KeyMoidInfo]),
			eredis:q(RedisClient,["DEL",KeyE64Info]),
			eredis:q(RedisClient,["DEL",KeyRunningInfo]),
			eredis:q(RedisClient,["EXEC"]),
			
			{ok,success};
		{error,_Reason} ->
			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",Key,DevMoid]),
			eredis:q(RedisClient,["DEL",KeyMoidInfo]),
			eredis:q(RedisClient,["DEL",KeyRunningInfo]),
			eredis:q(RedisClient,["EXEC"]),
			
			{ok,success}
	end.

%% 添加/更新终端设备的会议详情信息中给出的 channel 详细信息
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_channel_detail(pid(),string(),atom(),list(),list()) -> {ok,binary()}.
add_terminal_channel_detail(RedisClient,DevMoid,ChannelType,ChanID,InfoList) ->
	Key = format_key_channel( DevMoid, ChannelType, ChanID ),
	eredis:q(RedisClient,["HMSET",Key|InfoList]).

%% 添加/更新终端设备的会议详情信息中给出的 channel 编号
%% 返回值 : {ok,<<"N">>}
-spec add_terminal_meeting_channel(pid(),string(),atom(),list()) -> {ok,binary()}.
add_terminal_meeting_channel(RedisClient,DevMoid,ChannelType,ChanList) ->
	Key = format_key_meeting_channel( DevMoid, ChannelType ),
	eredis:q(RedisClient,["SADD",Key|ChanList]).


transform_list(_, Acc, []) ->
	Acc;
transform_list(Base, Acc, [H|T]) ->
	transform_list(Base, [Base++":"++binary_to_list(H)|Acc], T).

del_channels_info(_,_,[]) ->
	{ok,<<"OK">>};
del_channels_info(RedisClient,TerminalMoid,[ChanType|Rest]) ->	
	SetKey = format_key_meeting_channel(TerminalMoid, ChanType),
	case eredis:q(RedisClient,["SMEMBERS",SetKey]) of
		{ok, ChanIndexes} ->
			HashKeyList = transform_list(SetKey, [], ChanIndexes),
			eredis:q(RedisClient,["DEL"|HashKeyList]),
			eredis:q(RedisClient,["DEL",SetKey]),
			del_channels_info(RedisClient, TerminalMoid, Rest);
		Ret ->
			lager:warning("'Error ~p' -- ~p", [SetKey, Ret])
	end.

%% 删除终端设备的会议详情信息中给出的所有 channel 相关信息
%% terminal:devid:meetingdetail:privideo_send_chan
%% terminal:devid:meetingdetail:privideo_recv_chan
%% terminal:devid:meetingdetail:assvideo_send_chan
%% terminal:devid:meetingdetail:assvideo_recv_chan
%% terminal:devid:meetingdetail:audio_send_chan
%% terminal:devid:meetingdetail:audio_recv_chan
%% 返回值 : {ok,<<"OK">>}
-spec del_terminal_meeting_channels(pid(),string()) -> {ok,binary()}.
del_terminal_meeting_channels(RedisClient,TerminalMoid) ->

	ChannelTypeList = [
					       privideo_send_chan,
					       privideo_recv_chan,
					       assvideo_send_chan,
					       assvideo_recv_chan,
					       audio_send_chan,
					       audio_recv_chan
					  ],
	del_channels_info(RedisClient,TerminalMoid,ChannelTypeList).


%% 添加/更新终端设备的会议详情信息(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_meeting_detail(pid(),string(),string(),string(),string()) -> {ok,binary()}.	
add_terminal_meeting_detail( RedisClient,DevMoid,MT_E164,Conf_E164,ConfBitRate ) ->
	Key = format_key_meeting_detail( DevMoid ),
	KeyValuePairs = [
						Key,
						"moid",			DevMoid,
						"mt_e164",		MT_E164,
						"conf_e164",	Conf_E164,
						"conf_bitrate",	ConfBitRate
					],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 删除终端设备的会议详情信息(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec del_terminal_meeting_detail(pid(),string()) -> {ok,binary()}.	
del_terminal_meeting_detail( RedisClient,DevMoid ) ->
	Key = format_key_meeting_detail( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加/更新终端设备的运行信息(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_running_info(pid(),string(),string(),string(),string(),string(),string(),string(),string()) -> {ok,binary()}.	
add_terminal_running_info( RedisClient,DevMoid,Type,Version,OS,CpuType,CpuFreq,CpuNum,Memory ) ->
	Key = format_key_running_info( DevMoid ),
	KeyValuePairs = [
						Key,
						"moid",		DevMoid,
						"type",		Type,
						"version",	Version,
						"os",		OS,
						"cpu_type",	CpuType,
						"cpu_freq",	CpuFreq,
						"cpu_num",	CpuNum,
						"memory",	Memory
					],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).
	
%% 删除终端设备的运行信息(散列类型数据)
%% 返回值 : {ok,<<"1">>}
-spec del_terminal_running_info(pid(),string()) -> {ok,binary()}.	
del_terminal_running_info( RedisClient,DevMoid ) ->
	Key = format_key_running_info( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加/更新终端设备的带宽信息(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_bandwidth_info(pid(),string(),string(),string(),string(),string()) -> {ok,binary()}.	
add_terminal_bandwidth_info( RedisClient,DevMoid,SendBandWidth,SendDropRate,RecvBandWidth,RecvDropRate ) ->
	Key = format_key_net_info( DevMoid ),
	KeyValuePairs = [
						Key,
						"moid",				DevMoid,
						"send_bandwidth",	SendBandWidth,
						"send_droprate",	SendDropRate,
						"recv_bandwidth",	RecvBandWidth,
						"recv_droprate",	RecvDropRate
					],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 添加/更新终端设备的 APS 和网络信息(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_aps_and_net_info(pid(),string(),string(),string(),string(),string(),string()) -> {ok,binary()}.	
add_terminal_aps_and_net_info( RedisClient,DevMoid,TerIp,TerNatIp,TerDns,APSDomainName,ApsIP ) ->
	Key = format_key_net_info( DevMoid ),
	KeyValuePairs = [
						Key,
						"moid",				DevMoid,
						"ip", 				TerIp,
						"nat_ip", 			TerNatIp,
						"dns",				TerDns,
						"aps_domain",		APSDomainName,
						"aps_ip",			ApsIP
					],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 添加/更新终端设备的网络信息(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_net_info(pid(),string(),string(),string(),string()) -> {ok,binary()}.	
add_terminal_net_info( RedisClient,DevMoid,TerIp,TerNatIp,TerDns ) ->
	Key = format_key_net_info( DevMoid ),
	KeyValuePairs = [
						Key,
						"moid",				DevMoid,
						"ip", 				TerIp,
						"nat_ip", 			TerNatIp,
						"dns",				TerDns
					],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).
	
%% 删除终端设备的网络信息(散列类型数据)
%% 返回值 : {ok,<<"1">>}
-spec del_terminal_net_info(pid(),string()) -> {ok,binary()}.	
del_terminal_net_info( RedisClient,DevMoid ) ->
	Key = format_key_net_info( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加/更新终端设备的资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_resource(pid(),string(),integer(),integer(),integer()) -> {ok,binary()}.
add_terminal_resource( RedisClient,DevMoid,Cpu,Disk,Memory ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"cpu",Cpu,"disk",Disk,"memory",Memory],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 删除终端设备的资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"1">>}
-spec del_terminal_resource(pid(),string()) -> {ok,binary()}.
del_terminal_resource( RedisClient,DevMoid ) ->
	Key = format_key_resource( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).
	
%% 为终端设备删除所有告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_terminal_warning_all(pid(),string()) -> {ok,binary()}.	
del_terminal_warning_all( RedisClient,DevMoid ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 为终端设备添加一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec add_terminal_warning(pid(),string(),integer()) -> {ok,binary()}.
add_terminal_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SADD",Key,WarningCode]).
	
%% 为终端设备删除一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_terminal_warning(pid(),string(),integer()) -> {ok,binary()}.
del_terminal_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SREM",Key,WarningCode]).
	
%% 添加终端设备的在线状态(字符串类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec add_terminal_online(pid(),string()) -> {ok,binary()}.	
add_terminal_online( RedisClient,DevMoid ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["SET",Key,"online"]).

%% 删除终端设备的在线状态(字符串类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_terminal_online(pid(),string()) -> {ok,binary()}.	
del_terminal_online( RedisClient,DevMoid ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加需要连接的所有服务器类型(散列类型数据)
%% 返回值 : [{ok, return_value()} | {error, Reason::binary()}] | {error, no_connection}
%% ServerTypeList : 需要连接的服务器类型列表 
-spec add_terminal_connections(pid(),string(),list()) -> {ok,success}.	
add_terminal_connections( RedisClient,DevMoid,ServerTypeList ) ->
	Key = format_key_connection(DevMoid),
	Fun = fun( ConnectServerType, Acc ) ->
			[["HSET",Key,ConnectServerType,""]|Acc]
		  end,

	Pipeline = lists:foldl( Fun,[],ServerTypeList ),
	eredis:qp(RedisClient,Pipeline).

%% 删除服务器的连接情况(散列类型数据)	
%% 返回值 : {ok,<<"1">>} 
-spec del_terminal_connections(pid(),string()) -> {ok,binary()}.
del_terminal_connections( RedisClient,DevMoid ) ->
	Key = format_key_connection( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).
	
%% 添加一个需要连接的所有服务器类型(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec add_terminal_connection(pid(),string(),string()) -> {ok,binary()}.	
add_terminal_connection( RedisClient,DevMoid,ServerType ) ->
	Key = format_key_connection(DevMoid),
	eredis:q(RedisClient,["HSET",Key,ServerType,""]).
	
%% 删除一个需要连接的所有服务器类型(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_terminal_connection(pid(),string(),string()) -> {ok,binary()}.
del_terminal_connection( RedisClient,DevMoid,ServerType ) ->
	Key = format_key_connection( DevMoid ),
	eredis:q(RedisClient,["HDEL",Key,ServerType]).
	
%% 更新多个服务器类型的连接状态(散列类型数据)
%% 返回值 : [{ok, return_value()} | {error, Reason::binary()}] | {error, no_connection}
%% ServerConInfoList : 当前已连接服务器 IP 地址信息 [{ServerType,IP}, ...]
-spec update_terminal_connections(pid(),string(),list()) -> {ok,success}.
update_terminal_connections( RedisClient,DevMoid,ServerConInfoList ) ->
	Key = format_key_connection(DevMoid),
	
	Fun = fun( {ServerType,IP}, Acc ) ->
			[["HSET",Key,ServerType,IP]|Acc]
	      end,
	Pipeline = lists:foldl( Fun,[],ServerConInfoList ),
	eredis:qp(RedisClient,Pipeline).

%% 更新一个服务器类型的连接状态(散列类型数据)
%% 返回值 : {ok,<<"1">>}
-spec update_terminal_connection(pid(),string(),string(),string()) -> {ok,binary()}.
update_terminal_connection( RedisClient,DevMoid,ConnectServerType,IP ) ->
	Key = format_key_connection(DevMoid),
	eredis:q(RedisClient,["HSET",Key,ConnectServerType,IP]).
	
%% 获取指定域下的所有终端设备(集合类型数据)
%% 返回值 : TerminalList 
-spec get_all_terminal(pid(),string()|binary()) -> list().
get_all_terminal( RedisClient,DomainMoid ) ->
	Key = format_key_terminal( DomainMoid ),
	{ok,TerminalList} = eredis:q( RedisClient, ["SMEMBERS",Key] ),
	TerminalList.
	
%% 获取指定域下的终端设备总数(集合类型数据)
%% 返回值 : Count 
-spec get_terminal_count(pid(),string()|binary()) -> integer().
get_terminal_count( RedisClient,DomainMoid ) ->
	Key = format_key_terminal( DomainMoid ),
	{ok,Count} = eredis:q( RedisClient, ["SCARD",Key] ),
	list_to_integer(binary_to_list(Count)).
	
%% 通过moid获取指定终端设备的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok,{Moid,DomainMoid,Name,E164}}
-spec get_terminal_base_info(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary()}.
get_terminal_base_info( RedisClient, DevMoid ) ->
	KeyInfo = format_key_base_info( DevMoid ),
	case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4}],
			{<<"moid">>,Moid}              = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"domain_moid">>,DomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}              = lists:keyfind(<<"name">>,1,ValueList),
			{<<"e164">>,E164}              = lists:keyfind(<<"e164">>,1,ValueList),

			{ok, {Moid,DomainMoid,Name,E164}}
	end.
	
%% 通过e164号码获取指定终端设备的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok,{Moid,DomainMoid,Name,E164}}
-spec get_terminal_base_info_by_e164(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary()}.
get_terminal_base_info_by_e164( RedisClient, DevE164 ) ->
	KeyInfo = format_key_base_info_by_e164( DevE164 ),
	case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4}],
			{<<"moid">>,Moid}              = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"domain_moid">>,DomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}              = lists:keyfind(<<"name">>,1,ValueList),
			{<<"e164">>,E164}              = lists:keyfind(<<"e164">>,1,ValueList),

			{ok, {Moid,DomainMoid,Name,E164}}
	end.
	
%% 获取指定终端设备的运行信息(散列类型数据)
%% 返回值 : {Moid,Type,Version,OS,CpuType,CpuFreq,CpuNum,Memory}
-spec get_terminal_running_info(pid(),string()|binary())->{binary(),binary(),binary(),binary(),binary(),binary(),binary(),binary()}.
get_terminal_running_info( RedisClient, DevMoid ) ->
	KeyInfo = format_key_running_info( DevMoid ),
	ValueList = case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
					{ok,[]} ->
						[];
					{ok,[Key1,Value1]} ->
						[{Key1,Value1}];
					{ok,[Key1,Value1,Key2,Value2]} ->
						[{Key1,Value1},{Key2,Value2}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7,Key8,Value8]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7},{Key8,Value8}]
				end,

	MoidResult   = lists:keyfind(<<"moid">>,1,ValueList),
	TypeResult   = lists:keyfind(<<"type">>,1,ValueList),
	VersionResult= lists:keyfind(<<"version">>,1,ValueList),
	OSResult     = lists:keyfind(<<"os">>,1,ValueList),
	CpuTypeResult= lists:keyfind(<<"cpu_type">>,1,ValueList),
	CpuFreqResult= lists:keyfind(<<"cpu_freq">>,1,ValueList),
	CpuNumResult = lists:keyfind(<<"cpu_num">>,1,ValueList),
	MemoryResult = lists:keyfind(<<"memory">>,1,ValueList),

	Moid = case MoidResult of
			false ->
				<<"">>;
			{<<"moid">>,MoidValue} ->
				MoidValue
		 end,
	Type = case TypeResult of
				false ->
					<<"">>;
				{<<"type">>,TypeValue} ->
					TypeValue
		   end,
	Version = case VersionResult of
				false ->
					<<"">>;
				{<<"version">>,VersionValue} ->
					VersionValue
		     end,

	OS = case OSResult of
			false ->
				<<"">>;
			{<<"os">>,OSValue} ->
				OSValue
		 end,
	CpuType = case CpuTypeResult of
				false ->
					<<"">>;
				{<<"cpu_type">>,CpuTypeValue} ->
					CpuTypeValue
		      end,
	CpuFreq = case CpuFreqResult of
				false ->
					<<"">>;
				{<<"cpu_freq">>,CpuFreqValue} ->
					CpuFreqValue
		     end,
	CpuNum = case CpuNumResult of
				false ->
					<<"">>;
				{<<"cpu_num">>,CpuNumValue} ->
					CpuNumValue
		     end,
	Memory = case MemoryResult of
				false ->
					<<"">>;
				{<<"memory">>,MemoryValue} ->
					MemoryValue
		     end,
	{Moid,Type,Version,OS,CpuType,CpuFreq,CpuNum,Memory}.

%% 获取指定终端设备的网络信息(散列类型数据)
%% 返回值 : {Moid,Type,IP,Version}
-spec get_terminal_net_info(pid(),string()|binary()) ->{binary(),binary(),binary(),binary(),binary(),binary(),binary(),binary(),binary()}.
get_terminal_net_info( RedisClient, DevMoid ) ->
	KeyInfo = format_key_net_info( DevMoid ),
	ValueList = case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
					{ok,[]} ->
						[];
					{ok,[Key1,Value1]} ->
						[{Key1,Value1}];
					{ok,[Key1,Value1,Key2,Value2]} ->
						[{Key1,Value1},{Key2,Value2}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7,Key8,Value8]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7},{Key8,Value8}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7,Key8,Value8,Key9,Value9]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7},{Key8,Value8},{Key9,Value9}]
				end,

	MoidResult       = lists:keyfind(<<"moid">>,1,ValueList),
	IPResult         = lists:keyfind(<<"ip">>,1,ValueList),
	DnsResult        = lists:keyfind(<<"dns">>,1,ValueList),
	ApsDomainResult  = lists:keyfind(<<"aps_domain">>,1,ValueList),
	ApsIPResult      = lists:keyfind(<<"aps_ip">>,1,ValueList),
	SBandWidthResult = lists:keyfind(<<"send_bandwidth">>,1,ValueList),
	SDropRateResult  = lists:keyfind(<<"send_droprate">>,1,ValueList),
	RBandWidthResult = lists:keyfind(<<"recv_bandwidth">>,1,ValueList),
	RDropRateResult  = lists:keyfind(<<"recv_droprate">>,1,ValueList),

	Moid = case MoidResult of
			false ->
				<<"">>;
			{<<"moid">>,MoidValue} ->
				MoidValue
		 end,
	IP = case IPResult of
			false ->
				<<"">>;
			{<<"ip">>,IPValue} ->
				IPValue
		 end,
	Dns = case DnsResult of
				false ->
					<<"">>;
				{<<"dns">>,DnsValue} ->
					DnsValue
		     end,

	ApsDomain = case ApsDomainResult of
					false ->
						<<"">>;
					{<<"aps_domain">>,ApsDomainValue} ->
						ApsDomainValue
				end,
	ApsIP = case ApsIPResult of
				false ->
					<<"">>;
				{<<"aps_ip">>,ApsIPValue} ->
					ApsIPValue
	        end,
	SBandWidth = case SBandWidthResult of
					false ->
						<<"">>;
					{<<"send_bandwidth">>,SBandWidthValue} ->
						SBandWidthValue
			     end,
	SDropRate = case SDropRateResult of
					false ->
						<<"">>;
					{<<"send_droprate">>,SDropRateValue} ->
						SDropRateValue
		        end,
	RBandWidth = case RBandWidthResult of
					false ->
						<<"">>;
					{<<"recv_bandwidth">>,RBandWidthValue} ->
						RBandWidthValue
			     end,
	RDropRate = case RDropRateResult of
					false ->
						<<"">>;
					{<<"recv_droprate">>,RDropRateValue} ->
						RDropRateValue
		        end,
	{Moid,IP,Dns,ApsDomain,ApsIP,SBandWidth,SDropRate,RBandWidth,RDropRate}.

%% 获取指定终端设备的资源使用情况(散列类型数据)
%% 返回值 : {Moid,Type,IP,Version}
-spec get_terminal_resource(pid(),string()|binary()) -> {binary(),binary(),binary()}.
get_terminal_resource( RedisClient, DevMoid ) ->
	KeyResource = format_key_resource( DevMoid ),
	ValueList = case eredis:q(RedisClient,["HGETALL",KeyResource]) of
					{ok,[]} ->
						[];
					{ok,[Key1,Value1]} ->
						[{Key1,Value1}];
					{ok,[Key1,Value1,Key2,Value2]} ->
						[{Key1,Value1},{Key2,Value2}];
					{ok,[Key1,Value1,Key2,Value2,Key3,Value3]} ->
						[{Key1,Value1},{Key2,Value2},{Key3,Value3}]
				end,

	CpuResult       = lists:keyfind(<<"cpu">>,1,ValueList), 
	DiskResult      = lists:keyfind(<<"disk">>,1,ValueList), 
	MemoryResult    = lists:keyfind(<<"memory">>,1,ValueList), 

	CPU = case CpuResult of
			false ->
				0;
			{<<"cpu">>,CpuValue} ->
				CpuValue
		  end,
	DISK = case DiskResult of
				false ->
					0;
				{<<"disk">>,DiskValue} ->
					DiskValue
		   end,
	Memory = case MemoryResult of
				false ->
					0;
				{<<"memory">>,MemoryValue} ->
					MemoryValue
		     end,

	{CPU,DISK,Memory}.
	
%% 获取指定终端设备的在线状态信息(字符串类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok,<<"online">>} 
-spec get_terminal_online(pid(),string()|binary()) -> {error,binary()}|{ok,binary()}.
get_terminal_online( RedisClient, DevMoid ) ->
	Key = format_key_online( DevMoid ),
	case eredis:q(RedisClient,["GET",Key]) of
		{ok,undefined} ->
			{error,<<"Key Error">>};
		{ok,Value} ->
			{ok,Value}
	end.

%% 获取指定终端设备的所有告警码(集合类型数据)
%% 返回值 : CodeList 
-spec get_terminal_warning(pid(),string()|binary()) -> list().	
get_terminal_warning( RedisClient, DevMoid ) ->
	Key = format_key_warning( DevMoid ),
	{ok,CodeList} = eredis:q(RedisClient,["SMEMBERS",Key]),
	CodeList.
	
%% 获取指定终端设备与服务器的连接状态信息(集合类型数据)
%% 返回值 : [Key1,Value1,Key2,Value2,...]
-spec get_terminal_connections(pid(),string()|binary()) -> list().
get_terminal_connections( RedisClient, DevMoid ) ->
	Key = format_key_connection( DevMoid ),
	{ok,ServerTypeList} = eredis:q( RedisClient, ["HKEYS",Key] ),
	case ServerTypeList of
		[] ->
			[];
		_ ->
			Fun = fun( Type,Acc ) ->
					{ok,IP} = eredis:q( RedisClient, ["HGET",Key,Type] ),
					[[Type,IP]|Acc]
			      end,
			ServerList = lists:foldl( Fun,[],ServerTypeList ),
			ServerList
	end.

format_key_terminal(DomainMoid) when is_list(DomainMoid)->"domain:" ++ DomainMoid ++ ":terminal";
format_key_terminal(DomainMoid) when is_binary(DomainMoid)->"domain:" ++ binary_to_list(DomainMoid) ++ ":terminal".

format_key_base_info(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":baseinfo";
format_key_base_info(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":baseinfo".

format_key_base_info_by_e164(E164) when is_list(E164)->"terminal:" ++ E164 ++ ":baseinfo";
format_key_base_info_by_e164(E164) when is_binary(E164)->"terminal:" ++ binary_to_list(E164) ++ ":baseinfo".

format_key_channel(DevMoid,ChannelType,ChanID) when is_list(DevMoid) ->
	"terminal:" ++ DevMoid ++ ":meetingdetail:" ++ atom_to_list(ChannelType) ++ ":" ++ ChanID;
format_key_channel(DevMoid,ChannelType,ChanID) when is_binary(DevMoid) ->
	"terminal:" ++ binary_to_list(DevMoid) ++ ":meetingdetail:" ++ atom_to_list(ChannelType) ++ ":" ++ ChanID.

format_key_meeting_channel(DevMoid,ChannelType) when is_list(DevMoid) andalso is_atom(ChannelType) ->
	"terminal:" ++ DevMoid ++ ":meetingdetail:" ++ atom_to_list(ChannelType);
format_key_meeting_channel(DevMoid,ChannelType) when is_binary(DevMoid) andalso is_atom(ChannelType) ->
	"terminal:" ++ binary_to_list(DevMoid) ++ ":meetingdetail:" ++ atom_to_list(ChannelType).

format_key_meeting_detail(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":meetingdetail";
format_key_meeting_detail(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":meetingdetail".

format_key_running_info(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":runninginfo";
format_key_running_info(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":runninginfo".

format_key_net_info(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":netinfo";
format_key_net_info(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":netinfo".

format_key_resource(DevMoid) when is_list(DevMoid) ->"terminal:" ++ DevMoid ++ ":resource";
format_key_resource(DevMoid) when is_binary(DevMoid) ->"terminal:" ++ binary_to_list(DevMoid) ++ ":resource".

format_key_online(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":online";
format_key_online(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":online".

format_key_warning(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":warning";
format_key_warning(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":warning".

format_key_connection(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":connection";
format_key_connection(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":connection".