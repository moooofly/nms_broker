-module(physical_server_handler).

-export([add_physical_server/6,
         del_physical_server/3,
         update_physical_server_cpu_resource/3,   %% add 
         update_physical_server_disk_resource/3,  %% add
         update_physical_server_mem_resource/3,   %% add
         update_physical_server_net_resource/4,   %% add
		 update_physical_server_resource/7,
		 del_physical_server_resource/2,
		 add_physical_server_warning/3,
		 del_physical_server_warning/3,
		 update_physical_server_online/3,
         get_all_physical_server/2,
		 get_physical_server_count/2,
         get_physical_server_info/2,
		 get_physical_server_resource/2,
		 get_physical_server_warning/2,
		 get_physical_server_warning_count/2,
		 get_physical_server_online/2,
         get_all_logic_server/2]).
	
%% 添加一个物理服务器(散列类型数据)
%% 添加物理服务器详细信息的同时,需要把物理服务器的moid添加到物理服务器所属域的物理服务器列表里面去
%% 返回值 : {ok,success} 
-spec add_physical_server(pid(),string(),string(),string(),string(),string()) -> {ok,success}.	
add_physical_server( RedisClient,DevMoid,DomainMoid,Name,Location,IP ) ->
	UName = binary_to_list(unicode:characters_to_binary(Name)),
	ULocation = binary_to_list(unicode:characters_to_binary(Location)),
	
	KeyServer = format_key_server( DomainMoid ),
	KeyInfo = format_key_info( DevMoid ),
	KeyValuePairs = [KeyInfo,"moid",DevMoid,"domain_moid",DomainMoid,"name",UName,"location",ULocation,"ip",IP],
	
	eredis:q(RedisClient,["MULTI"]),
	eredis:q(RedisClient,["SADD",KeyServer,DevMoid]),
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]),
	eredis:q(RedisClient,["EXEC"]),
	
	{ok,success}.
	
%% 删除一个物理服务器(散列类型数据)
%% 返回值 : {ok,success}
-spec del_physical_server(pid(),string(),string()) -> {ok,success}.	
del_physical_server( RedisClient,DevMoid,DomainMoid ) ->
	KeyServer = format_key_server( DomainMoid ),
	KeyInfo = format_key_info( DevMoid ),
	
	eredis:q(RedisClient,["MULTI"]),
	eredis:q(RedisClient,["SREM",KeyServer,DevMoid]),
	eredis:q(RedisClient,["DEL",KeyInfo]),
	eredis:q(RedisClient,["EXEC"]),
	
	{ok,success}.



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

%% 更新物理服务器的资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"OK">>} 
-spec update_physical_server_resource(pid(),string(),integer(),integer(),integer(),integer(),integer()) -> {ok,binary()}.
update_physical_server_resource( RedisClient,DevMoid,Cpu,Disk,Memory,PortIn,PortOut ) ->
	Key = format_key_resource( DevMoid ),
	KeyValuePairs = [Key,"cpu",Cpu,"disk",Disk,"memory",Memory,"portin",PortIn,"portout",PortOut],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).
	
%% 删除物理服务器的资源使用情况(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_physical_server_resource(pid(),string() ) -> {ok,binary()}.
del_physical_server_resource( RedisClient,DevMoid ) ->
	Key = format_key_resource( DevMoid ),
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
	
%% 更新物理服务器的在线状态(字符串类型数据)
%% 返回值  {ok,<<"OK">>} 
-spec update_physical_server_online(pid(),string(),integer()) -> {ok,binary()}.	
update_physical_server_online( RedisClient,DevMoid,ExpireTime ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["SET",Key,"online","EX",ExpireTime]).
	
%% 获取指定平台域下的所有物理服务器设备(集合类型数据)
%% 返回值 : ServerList
-spec get_all_physical_server(pid(),string()|binary()) -> list().		
get_all_physical_server( RedisClient, DomainMoid ) ->
    io:format("[get_all_physical_server]the domain id is:~p~n",[DomainMoid]),
	Key = format_key_server( DomainMoid ),
	{ok,ServerList} = eredis:q( RedisClient, ["SMEMBERS",Key] ),
	ServerList.
	
%% 获取指定平台域下的物理服务器设备总数(集合类型数据)
%% 返回值 : integer()
-spec get_physical_server_count(pid(),string()|binary()) -> integer().		
get_physical_server_count( RedisClient, DomainMoid ) ->
    io:format("[get_physical_server_count]the domain id is:~p~n",[DomainMoid]),
	Key = format_key_server( DomainMoid ),
	{ok,Count} = eredis:q( RedisClient, ["SCARD",Key] ),
	list_to_integer(binary_to_list(Count)).
	
%% 获取指定物理服务器的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{Moid,DomainMoid,Name,Location,IP}
-spec get_physical_server_info(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary(),binary()}.
get_physical_server_info( RedisClient,DevMoid ) ->
	Key = format_key_info( DevMoid ),
	case eredis:q(RedisClient,["HGETALL",Key]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
		{ok,[<<"moid">>,Moid,<<"guid">>,Guid,<<"domain_moid">>,DomainMoid,<<"name">>,Name,<<"location">>,Location,<<"ip">>,IP]} ->
			{Moid,Guid,DomainMoid,Name,Location,IP}
	end.

%% 获取指定物理服务器的资源使用情况(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{CPU,DISK,Memory,PortIn,PortOut}
-spec get_physical_server_resource(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary(),binary()}.	
get_physical_server_resource( RedisClient, DevMoid ) ->
	Key = format_key_resource( DevMoid ),
	case eredis:q(RedisClient,["HGETALL",Key]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
		{ok,[<<"cpu">>,CPU,<<"disk">>,DISK,<<"memory">>,Memory,<<"portin">>,PortIn,<<"portout">>,PortOut]} ->
			{CPU,DISK,Memory,PortIn,PortOut}
	end.

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
	
format_key_server(DomainMoid) when is_list(DomainMoid) ->"domain:" ++ DomainMoid ++ ":server";
format_key_server(DomainMoid) when is_binary(DomainMoid) ->"domain:" ++ binary_to_list(DomainMoid) ++ ":server".

format_key_info(DevMoid) when is_list(DevMoid) ->"p_server:" ++ DevMoid ++ ":info";
format_key_info(DevMoid) when is_binary(DevMoid) ->"p_server:" ++ binary_to_list(DevMoid) ++ ":info".

format_key_resource(DevMoid) when is_list(DevMoid) ->"p_server:" ++ DevMoid ++ ":resource";
format_key_resource(DevMoid) when is_binary(DevMoid) ->"p_server:" ++ binary_to_list(DevMoid) ++ ":resource".

format_key_warning(DevMoid) when is_list(DevMoid) ->"p_server:" ++ DevMoid ++ ":warning";
format_key_warning(DevMoid) when is_binary(DevMoid) ->"p_server:" ++ binary_to_list(DevMoid) ++ ":warning".

format_key_online(DevMoid) when is_list(DevMoid) ->"p_server:" ++ DevMoid ++ ":online";
format_key_online(DevMoid) when is_binary(DevMoid) ->"p_server:" ++ binary_to_list(DevMoid) ++ ":online".

format_key_logic_server(DevMoid) when is_list(DevMoid) ->"p_server:" ++ DevMoid ++ ":l_server";
format_key_logic_server(DevMoid) when is_binary(DevMoid) ->"p_server:" ++ binary_to_list(DevMoid) ++ ":l_server".