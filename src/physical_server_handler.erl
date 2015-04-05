-module(physical_server_handler).

-export([add_physical_server/7,
         del_physical_server/3,

         add_collectorid/2,                       %% add
         del_collectorid/2,                       %% add

         set_heartbeat_timer_by_collectorid/3,    %% add
         get_heartbeat_timer_by_collectorid/2,    %% add
         del_heartbeat_timer_by_collectorid/2,    %% add

         add_collector_online_device/4,           %% add
         del_collector_online_device/4,           %% add
         del_collector_online_device_all/2,       %% add

         add_physical_ip/3,                       %% add

         update_physical_server_cpu_resource/3,   %% add 
         update_physical_server_disk_resource/3,  %% add
         update_physical_server_mem_resource/3,   %% add
         update_physical_server_net_resource/4,   %% add
         add_physical_server_resource/7,
         del_physical_server_resource/2,

         del_physical_server_warning_all/2,       %% add
         add_physical_server_warning/3,
         del_physical_server_warning/3,

         add_physical_server_online/2,
         del_physical_server_online/2,

         get_all_physical_server/2,
         get_physical_server_count/2,
         get_physical_server_info_by_moid/2,
         get_physical_server_info_by_guid/2,     %% update
         get_physical_server_resource/2,
         get_physical_server_warning/2,
         get_physical_server_warning_count/2,
         get_physical_server_online/2,
         get_all_logic_server/2]).


%% 添加一个物理服务器(散列类型数据)
%% 添加物理服务器详细信息的同时,需要把物理服务器的moid添加到物理服务器所属域的物理服务器列表里面去
%% 返回值 : {ok,success} 
-spec add_physical_server(pid(),string(),string(),string(),string(),string(),string()) -> {ok,success}.	
add_physical_server( RedisClient,DevMoid,DevGuid,DomainMoid,Name,Location,IP ) ->
	UName = binary_to_list(unicode:characters_to_binary(Name)),
	ULocation = binary_to_list(unicode:characters_to_binary(Location)),
	
	KeyServer = format_key_server( DomainMoid ),
	KeyMoidInfo = format_key_moid_info( DevMoid ),
	KeyMoidValuePairs = [KeyMoidInfo,"moid",DevMoid,"guid",DevGuid,"domain_moid",DomainMoid,"name",UName,"location",ULocation,"ip",IP],

	KeyGuidInfo = format_key_guid_info( DevGuid ),
	KeyGuidValuePairs = [KeyGuidInfo,"moid",DevMoid,"guid",DevGuid,"domain_moid",DomainMoid,"name",UName,"location",ULocation,"ip",IP],
	
	eredis:q(RedisClient,["MULTI"]),
	eredis:q(RedisClient,["SADD",KeyServer,DevMoid]),
	eredis:q(RedisClient,["HMSET"|KeyMoidValuePairs]),
	eredis:q(RedisClient,["HMSET"|KeyGuidValuePairs]),
	eredis:q(RedisClient,["EXEC"]),
	
	{ok,success}.
	
%% 删除一个物理服务器(散列类型数据)
%% 返回值 : {ok,success}
-spec del_physical_server(pid(),string(),string()) -> {ok,success}.	
del_physical_server( RedisClient,DevMoid,DomainMoid ) ->
    KeyServer = format_key_server( DomainMoid ),
    KeyMoidInfo = format_key_moid_info( DevMoid ),

    case get_physical_server_info_by_moid(RedisClient,DevMoid) of
        {_,DevGuid,_,_,_,_} ->
    	    KeyGuidInfo = format_key_guid_info( DevGuid ),

            eredis:q(RedisClient,["MULTI"]),
            eredis:q(RedisClient,["SREM",KeyServer,DevMoid]),
            eredis:q(RedisClient,["DEL",KeyMoidInfo]),
            eredis:q(RedisClient,["DEL",KeyGuidInfo]),
            eredis:q(RedisClient,["EXEC"]),
			
            {ok,success};
        {error,_Reason} ->
            eredis:q(RedisClient,["MULTI"]),
            eredis:q(RedisClient,["SREM",KeyServer,DevMoid]),
            eredis:q(RedisClient,["DEL",KeyMoidInfo]),
            eredis:q(RedisClient,["EXEC"]),

            {ok,success}
    end.


-spec add_collectorid(pid(),string()) -> {ok,binary()}.
add_collectorid( RedisClient, CollectorID ) ->
	Key = "collector",
	eredis:q(RedisClient,["SADD",Key,CollectorID]).

-spec del_collectorid(pid(),string()) -> {ok,binary()}.
del_collectorid( RedisClient, CollectorID ) ->
	Key = "collector",
	eredis:q(RedisClient,["SREM",Key,CollectorID]).

%% 保存 collector 所使用的 heartbeat 定时器
%% 返回值 : {ok,<<"1">>} 
-spec set_heartbeat_timer_by_collectorid(pid(),string(),string()) -> {ok,binary()}.
set_heartbeat_timer_by_collectorid( RedisClient, CollectorID, TimerRef ) ->
	Key = format_key_collector_timer( CollectorID ),
	eredis:q(RedisClient,["HSET",Key,"heartbeat",TimerRef]).

%% 获取 collector 所使用的 heartbeat 定时器
%% 返回值 : {ok,<<"Value">>} 
-spec get_heartbeat_timer_by_collectorid(pid(),string()) -> {ok,binary()}.
get_heartbeat_timer_by_collectorid( RedisClient, CollectorID ) ->
	Key = format_key_collector_timer( CollectorID ),
	eredis:q(RedisClient,["HGET",Key,"heartbeat"]).

%% 删除 collector 所使用的 heartbeat 定时器
%% 返回值 : {ok,<<"1">>} 
-spec del_heartbeat_timer_by_collectorid(pid(),string()) -> {ok,binary()}.
del_heartbeat_timer_by_collectorid( RedisClient, CollectorID ) ->
	Key = format_key_collector_timer( CollectorID ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加一个 collector 下属设备的在线信息(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec add_collector_online_device(pid(),string(),string(),string()) -> {ok,binary()}.
add_collector_online_device( RedisClient, CollectorID, DevGuid, DevType ) ->
	Key = format_key_collector_online( CollectorID ),
	eredis:q(RedisClient,["SADD",Key,DevType++":"++DevGuid]).

%% 删除一个 collector 下属设备的在线信息(散列类型数据)
%% 返回值 : 成功 {ok,<<"1">>} 失败 {ok,<<"0">>}
-spec del_collector_online_device(pid(),string(),string(),string()) -> {ok,binary()}.
del_collector_online_device( RedisClient, CollectorID, DevGuid, DevType ) ->
	Key = format_key_collector_online( CollectorID ),
	eredis:q(RedisClient,["SREM",Key,DevType++":"++DevGuid]).


%% 删除特定 collector 下属全部设备的在线信息
%% 返回值 : 成功 {ok,<<"1">>} 
-spec del_collector_online_device_all(pid(),string()) -> {ok,binary()}.
del_collector_online_device_all( RedisClient, CollectorID ) ->
	Key = format_key_collector_online( CollectorID ),
	case eredis:q(RedisClient,["SMEMBERS",Key]) of
		{ok, OnlineDevs} ->
			Fun = fun(OnlineDev) ->
					[DevType, DevMoid] = string:tokens(binary_to_list(OnlineDev), ":"),
					Type = case nms_task_control:devtype_distinguish(list_to_binary(DevType)) of
						{terminal, _} -> "terminal";
						{logical, _}  -> "l_server";
						{physical}    -> "p_server"
					end,
					lager:info("'DEL ~p'~n", [Type++":"++DevMoid++":online"]),
					%% 删除各类型设备的 DevType:DevMoid:online 的内容
					eredis:q(RedisClient,["DEL",Type++":"++DevMoid++":online"])
			end,
			lists:foreach(Fun, OnlineDevs);
		Ret ->
			lager:warning("'Error ~p' -- ~p~n", [Key, Ret])
	end,
	eredis:q(RedisClient,["DEL",Key]).

%% 添加物理服务器的 IP 地址信息(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
add_physical_ip(RedisClient,DevMoid,PhySerIPString) ->
	Key = format_key_guid_info( DevMoid ),
	eredis:q(RedisClient,["HSET",Key,"ip",PhySerIPString]).

%% 更新物理服务器的 CPU 资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>} 
-spec update_physical_server_cpu_resource(pid(),string(),integer()) -> {ok,binary()}.
update_physical_server_cpu_resource( RedisClient,DevMoid,Cpu ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"cpu",Cpu],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 更新物理服务器的 DISK 资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>} 
-spec update_physical_server_disk_resource(pid(),string(),integer()) -> {ok,binary()}.
update_physical_server_disk_resource( RedisClient,DevMoid,Disk ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"disk",Disk],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 更新物理服务器的 MEM 资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>} 
-spec update_physical_server_mem_resource(pid(),string(),integer()) -> {ok,binary()}.
update_physical_server_mem_resource( RedisClient,DevMoid,Memory ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"memory",Memory],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).

%% 更新物理服务器的 NETCARD 资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>} 
-spec update_physical_server_net_resource(pid(),string(),integer(),integer()) -> {ok,binary()}.
update_physical_server_net_resource( RedisClient,DevMoid,PortIn,PortOut ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"portin",PortIn,"portout",PortOut],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).
	
%% 添加/更新物理服务器的资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>} 
-spec add_physical_server_resource(pid(),string(),integer(),integer(),integer(),integer(),integer()) -> {ok,binary()}.
add_physical_server_resource( RedisClient,DevMoid,Cpu,Disk,Memory,PortIn,PortOut ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"cpu",Cpu,"disk",Disk,"memory",Memory,"portin",PortIn,"portout",PortOut],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).
	
%% 删除物理服务器的资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_physical_server_resource(pid(),string() ) -> {ok,binary()}.
del_physical_server_resource( RedisClient,DevMoid ) ->
	Key = format_key_resource( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 为物理服务器删除所有告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_physical_server_warning_all(pid(),string()) -> {ok,binary()}.	
del_physical_server_warning_all( RedisClient,DevMoid ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 为物理服务器添加一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>}
-spec add_physical_server_warning(pid(),string(),integer()) -> {ok,binary()}.	
add_physical_server_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SADD",Key,WarningCode]).
	
%% 为物理服务器删除一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_physical_server_warning(pid(),string(),integer()) -> {ok,binary()}.	
del_physical_server_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SREM",Key,WarningCode]).
	
%% 添加物理服务器的在线状态(字符串类型数据)
%% 返回值  {ok,<<"1">>} 
-spec add_physical_server_online(pid(),string()) -> {ok,binary()}.	
add_physical_server_online( RedisClient,DevMoid ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["SET",Key,"online"]).

%% 删除物理服务器的在线状态(字符串类型数据)
%% 返回值  {ok,<<"1">>} 
-spec del_physical_server_online(pid(),string()) -> {ok,binary()}.	
del_physical_server_online( RedisClient,DevMoid ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).
	
%% 获取指定平台域下的所有物理服务器设备(集合类型数据)
%% 返回值 : ServerList
-spec get_all_physical_server(pid(),string()|binary()) -> list().		
get_all_physical_server( RedisClient, DomainMoid ) ->
    lager:info("The domain id is:~p~n",[DomainMoid]),
	Key = format_key_server( DomainMoid ),
	{ok,ServerList} = eredis:q( RedisClient, ["SMEMBERS",Key] ),
	ServerList.
	
%% 获取指定平台域下的物理服务器设备总数(集合类型数据)
%% 返回值 : integer()
-spec get_physical_server_count(pid(),string()|binary()) -> integer().		
get_physical_server_count( RedisClient, DomainMoid ) ->
    lager:info("The domain id is:~p~n",[DomainMoid]),
	Key = format_key_server( DomainMoid ),
	{ok,Count} = eredis:q( RedisClient, ["SCARD",Key] ),
	list_to_integer(binary_to_list(Count)).
	
%% 根据moid获取指定物理服务器的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{Moid,Guid,DomainMoid,Name,Location,IP}
-spec get_physical_server_info_by_moid(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary(),binary(),binary()}.
get_physical_server_info_by_moid( RedisClient,DevMoid ) ->
	KeyInfo = format_key_moid_info( DevMoid ),
	case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5]} ->
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5}],
			{<<"moid">>,Moid}              = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}              = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"domain_moid">>,DomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}              = lists:keyfind(<<"name">>,1,ValueList),
			{<<"location">>,Location}      = lists:keyfind(<<"location">>,1,ValueList),
			
			{Moid,Guid,DomainMoid,Name,Location,<<"">>};
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6]} ->
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5},{Key6,Value6}],
			{<<"moid">>,Moid}              = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}              = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"domain_moid">>,DomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}              = lists:keyfind(<<"name">>,1,ValueList),
			{<<"location">>,Location}      = lists:keyfind(<<"location">>,1,ValueList),
			{<<"ip">>,IP}                  = lists:keyfind(<<"ip">>,1,ValueList),
			
			{Moid,Guid,DomainMoid,Name,Location,IP}
	end.

%% 根据guid获取指定物理服务器的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Non-Exist">>}|{Moid,Guid,DomainMoid,Name,Type,Location,IP}
-spec get_physical_server_info_by_guid(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary(),binary(),binary()}.
get_physical_server_info_by_guid( RedisClient,DevGuid ) ->
	KeyInfo = format_key_guid_info( DevGuid ),

	Moid =  case eredis:q(RedisClient,["HGET",KeyInfo,"moid"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, MoidValue}        -> MoidValue
			end,

	Guid =  case eredis:q(RedisClient,["HGET",KeyInfo,"guid"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, GuidValue}        -> GuidValue
			end,

	DomainMoid = case eredis:q(RedisClient,["HGET",KeyInfo,"domain_moid"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, DomainMoidValue}  -> DomainMoidValue
			end,

	Type =  case eredis:q(RedisClient,["HGET",KeyInfo,"type"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, TypeValue}        -> TypeValue
			end,

	Name = case eredis:q(RedisClient,["HGET",KeyInfo,"name"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, NameValue}        -> NameValue
			end,

	Location = case eredis:q(RedisClient,["HGET",KeyInfo,"location"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, LocationValue}    -> LocationValue
			end,

	IP = case eredis:q(RedisClient,["HGET",KeyInfo,"ip"]) of
				{error, no_connection} -> <<"">>;
				{ok, undefined}        -> <<"">>;
				{ok, IPValue}          -> IPValue
			end,

	{ok, {Moid,Guid,DomainMoid,Name,Type,Location,IP}}.


%% 获取指定物理服务器的资源使用情况(散列类型数据)
%% 返回值 : {CPU,DISK,Memory,PortIn,PortOut}
-spec get_physical_server_resource(pid(),string()|binary()) -> {binary(),binary(),binary(),binary(),binary()}.	
get_physical_server_resource( RedisClient, DevMoid ) ->
	KeyResource = format_key_resource( DevMoid ),
	ValueList = case eredis:q(RedisClient,["HGETALL",KeyResource]) of
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
						[{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5}]
				end,

	CpuResult     = lists:keyfind(<<"cpu">>,1,ValueList),
	DiskResult    = lists:keyfind(<<"disk">>,1,ValueList),
	MemoryResult  = lists:keyfind(<<"memory">>,1,ValueList),
	PortInResult  = lists:keyfind(<<"portin">>,1,ValueList),
	PortOutResult = lists:keyfind(<<"portout">>,1,ValueList),

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
	PortIn = case PortInResult of
				false ->
					0;
				{<<"portin">>,PortInValue} ->
					PortInValue
		     end,
	PortOut = case PortOutResult of
				false ->
					0;
				{<<"portout">>,PortOutValue} ->
					PortOutValue
		      end,

	{CPU,DISK,Memory,PortIn,PortOut}.

%% 获取指定物理服务器的所有告警码(集合类型数据)
%% 返回值 : CodeList
-spec get_physical_server_warning(pid(),string()|binary()) -> list().	
get_physical_server_warning( RedisClient, DevMoid ) ->
	Key = format_key_warning( DevMoid ),
	{ok,CodeList} = eredis:q( RedisClient, ["SMEMBERS",Key] ),
	CodeList.
	
%% 获取指定物理服务器的告警码个数(集合类型数据)
%% 返回值 : integer()
-spec get_physical_server_warning_count(pid(),string()|binary()) -> integer().	
get_physical_server_warning_count( RedisClient, DevMoid ) ->
	Key = format_key_warning( DevMoid ),
	{ok,Count} = eredis:q( RedisClient, ["SCARD",Key] ),
	list_to_integer(binary_to_list(Count)).
	
%% 获取指定物理服务器的在线状态(字符串类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok,Value}
-spec get_physical_server_online(pid(),string()|binary()) -> {error,binary()}|{ok,binary()}.		
get_physical_server_online( RedisClient, DevMoid ) ->
	Key = format_key_online( DevMoid ),
	case eredis:q(RedisClient,["GET",Key]) of
		{ok,undefined} ->
			{error,<<"Key Error">>};
		{ok,Value} ->
			{ok,Value}
	end.
	
%% 获取指定物理服务器上运行的所有逻辑服务器(集合类型数据)
%% 返回值 : LogicServerList
-spec get_all_logic_server(pid(),string()|binary()) -> list().
get_all_logic_server( RedisClient, DevMoid ) ->
	Key = format_key_logic_server( DevMoid ),
	{ok,LogicServerList} = eredis:q( RedisClient, ["SMEMBERS",Key] ),
	LogicServerList.


format_key_collector_timer(CollectorID) when is_list(CollectorID) -> "collector:" ++ CollectorID ++ ":timer";
format_key_collector_timer(CollectorID) when is_binary(CollectorID) -> "collector:" ++ binary_to_list(CollectorID) ++ ":timer".

format_key_collector_online(CollectorID) when is_list(CollectorID) -> "collector:" ++ CollectorID ++ ":online";
format_key_collector_online(CollectorID) when is_binary(CollectorID) -> "collector:" ++ binary_to_list(CollectorID) ++ ":online".
	
format_key_server(DomainMoid) when is_list(DomainMoid) -> "domain:" ++ DomainMoid ++ ":server";
format_key_server(DomainMoid) when is_binary(DomainMoid) -> "domain:" ++ binary_to_list(DomainMoid) ++ ":server".

format_key_moid_info(DevMoid) when is_list(DevMoid) -> "p_server:" ++ DevMoid ++ ":info";
format_key_moid_info(DevMoid) when is_binary(DevMoid) -> "p_server:" ++ binary_to_list(DevMoid) ++ ":info".

format_key_guid_info(DevGuid) when is_list(DevGuid) -> "p_server:" ++ DevGuid ++ ":info";
format_key_guid_info(DevGuid) when is_binary(DevGuid) -> "p_server:" ++ binary_to_list(DevGuid) ++ ":info".

format_key_resource(DevMoid) when is_list(DevMoid) -> "p_server:" ++ DevMoid ++ ":resource";
format_key_resource(DevMoid) when is_binary(DevMoid) -> "p_server:" ++ binary_to_list(DevMoid) ++ ":resource".

format_key_warning(DevMoid) when is_list(DevMoid) -> "p_server:" ++ DevMoid ++ ":warning";
format_key_warning(DevMoid) when is_binary(DevMoid) -> "p_server:" ++ binary_to_list(DevMoid) ++ ":warning".

format_key_online(DevMoid) when is_list(DevMoid) -> "p_server:" ++ DevMoid ++ ":online";
format_key_online(DevMoid) when is_binary(DevMoid) -> "p_server:" ++ binary_to_list(DevMoid) ++ ":online".

format_key_logic_server(DevMoid) when is_list(DevMoid) -> "p_server:" ++ DevMoid ++ ":l_server";
format_key_logic_server(DevMoid) when is_binary(DevMoid) -> "p_server:" ++ binary_to_list(DevMoid) ++ ":l_server".