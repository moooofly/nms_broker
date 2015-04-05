-module(logical_server_handler).

-export([add_logic_server/8,
         del_logic_server/3,

         add_xmpp_online_statistic/3,          %% add
         add_xmpp_in_all_domains/2,            %% add
         del_xmpp_in_all_domains/2,            %% add

         add_pas_in_all_domains/2,             %% add
         del_pas_in_all_domains/2,             %% add

         add_pas_online_statistic/6,           %% add
         add_pas_moid/3,                       %% add

         del_logic_server_warning_all/2,       %% add
         add_logic_server_warning/3,
		 del_logic_server_warning/3,

		 add_logic_server_online/2,
		 del_logic_server_online/2,

		 add_logical_server_warning/3,       %% update
		 del_logical_server_warning/3,       %% add

		 add_logic_server_connections/3,
		 del_logic_server_connections/2,
		 add_logic_server_connection/3,
		 del_logic_server_connection/3,		 
		 update_logic_server_connections/3,
		 update_logic_server_connection/4,

		 get_logic_server_info_by_moid/2,    %% update
		 get_logic_server_info_by_guid/2,    %% update

		 get_logic_server_online/2,
         get_logic_server_connections/2]).


%% 保存指定域下指定 XMPP 服务器在线数信息
%% 返回值 : {ok,<<"1">>} 
-spec add_xmpp_online_statistic(pid(),string(),integer()) -> {ok,binary()}.
add_xmpp_online_statistic( RedisClient,DomainMoid,OnlineNum ) ->
	Key = format_key_xmpp_online( DomainMoid ),
	eredis:q(RedisClient,["SET",Key,OnlineNum]).

%% 保存定时任务所需的 XMPP 服务器查询字串
%% 返回值 : {ok,<<"1">>} 
-spec add_xmpp_in_all_domains(pid(),string()) -> {ok,binary()}.
add_xmpp_in_all_domains( RedisClient,XmppDomainKey ) ->
	Key = "xmpp_in_all_domains",
	eredis:q(RedisClient,["SADD",Key,XmppDomainKey]).

%% 删除定时任务所需的 XMPP 服务器查询字串
%% 返回值 : {ok,<<"1">>} 
-spec del_xmpp_in_all_domains(pid(),string()) -> {ok,binary()}.
del_xmpp_in_all_domains( RedisClient,XmppDomainKey ) ->
	Key = "xmpp_in_all_domains",
	eredis:q(RedisClient,["SREM",Key,XmppDomainKey]).

%% 保存定时任务所需的 PAS 服务器查询字串
%% 返回值 : {ok,<<"1">>} 
-spec add_pas_in_all_domains(pid(),string()) -> {ok,binary()}.
add_pas_in_all_domains( RedisClient,CompletePasDomainInfo ) ->
	Key = "pas_in_all_domains",
	eredis:q(RedisClient,["SADD",Key,CompletePasDomainInfo]).

%% 删除定时任务所需的 PAS 服务器查询字串
%% 返回值 : {ok,<<"1">>} 
-spec del_pas_in_all_domains(pid(),string()) -> {ok,binary()}.
del_pas_in_all_domains( RedisClient,CompletePasDomainInfo ) ->
	Key = "pas_in_all_domains",
	eredis:q(RedisClient,["SREM",Key,CompletePasDomainInfo]).

%% 保存指定域下指定 PAS 服务器上的在线数信息
%% 返回值 : {ok,<<"1">>} 
-spec add_pas_online_statistic(pid(),string(),string(),integer(),integer(),integer()) -> {ok,binary()}.
add_pas_online_statistic( RedisClient,DomainMoid,DevMoid,H323Online,SIPOnline,MonitorOnline ) ->
	Key = format_key_pas_online( DomainMoid, DevMoid ),
	KeyMoidValuePairs = [
							"h323",    H323Online,
							"sip",     SIPOnline,
							"monitor", MonitorOnline
						],
	eredis:q(RedisClient,["HMSET",Key|KeyMoidValuePairs]).


%% 在指定域下添加 PAS 服务器的 moid 信息
%% 返回值 : {ok,<<"1">>} 
-spec add_pas_moid(pid(),string(),string()) -> {ok,binary()}.
add_pas_moid( RedisClient,DomainMoid,DevMoid ) ->
	Key = format_key_pas( DomainMoid ),
	eredis:q(RedisClient,["SADD",Key,DevMoid]).


%% 添加一个逻辑服务器(散列类型数据)
%% 需要把逻辑服务器添加到它所属物理服务器的逻辑服务器列表里面去
%% 返回值 : {ok,success} 
-spec add_logic_server(pid(),string(),string(),string(),string(),string(),string(),string()) -> {ok,success}.
add_logic_server( RedisClient,DevMoid,DevGuid,PServerMoid,DomainMoid,Name,IP,Type ) ->
    PServerKey = format_key_logic_server(PServerMoid),
	KeyMoidInfo = format_key_moid_info( DevMoid ),
	KeyMoidValuePairs = [KeyMoidInfo,"moid",DevMoid,"guid",DevGuid,"p_server_moid",PServerMoid,"domain_moid",DomainMoid,"name",Name,"ip",IP,"type",Type],
	
	KeyGuidInfo = format_key_guid_info( DevGuid ),
	KeyGuidValuePairs = [KeyGuidInfo,"moid",DevMoid,"guid",DevGuid,"p_server_moid",PServerMoid,"domain_moid",DomainMoid,"name",Name,"ip",IP,"type",Type],
	
	eredis:q(RedisClient,["MULTI"]),
	eredis:q(RedisClient,["SADD",PServerKey,DevMoid]),
	eredis:q(RedisClient,["HMSET"|KeyMoidValuePairs]),
	eredis:q(RedisClient,["HMSET"|KeyGuidValuePairs]),
	eredis:q(RedisClient,["EXEC"]),
	
	{ok,success}.
	
%% 删除一个逻辑服务器(散列类型数据)
%% 需要把逻辑服务器从它所属的物理服务器的逻辑服务器列表里面删除
%% 返回值 : {ok,success} 
-spec del_logic_server(pid(),string(),string()) -> {ok,success}.
del_logic_server( RedisClient,DevMoid,PServerMoid) ->
    PServerKey = format_key_logic_server(PServerMoid),
	KeyMoidInfo = format_key_moid_info( DevMoid ),

	case get_logic_server_info_by_moid(RedisClient,DevMoid) of
		{ok, {_,DevGuid,_,_,_,_,_}}->
			KeyGuidInfo = format_key_guid_info( DevGuid ),

			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",PServerKey,DevMoid]),
			eredis:q(RedisClient,["DEL",KeyMoidInfo]),
			eredis:q(RedisClient,["DEL",KeyGuidInfo]),
			eredis:q(RedisClient,["EXEC"]),
			
			{ok,success};
		{error,_Reason} ->
			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",PServerKey,DevMoid]),
			eredis:q(RedisClient,["DEL",KeyMoidInfo]),
			eredis:q(RedisClient,["EXEC"]),
			
			{ok,success}
	end.
	
%% 为逻辑服务器删除所有告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_logic_server_warning_all(pid(),string()) -> {ok,binary()}.	
del_logic_server_warning_all( RedisClient,DevMoid ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 为逻辑服务器添加一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec add_logic_server_warning(pid(),string(),integer()) -> {ok,binary()}.
add_logic_server_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SADD",Key,WarningCode]).
	
%% 为逻辑服务器删除一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_logic_server_warning(pid(),string(),integer()) -> {ok,binary()}.
del_logic_server_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SREM",Key,WarningCode]).
	
%% 添加逻辑服务器的在线状态(字符串类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec add_logic_server_online(pid(),string()) -> {ok,binary()}.
add_logic_server_online( RedisClient,DevMoid ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["SET",Key,"online"]).
	
%% 删除逻辑服务器的在线状态(字符串类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_logic_server_online(pid(),string()) -> {ok,binary()}.
del_logic_server_online( RedisClient,DevMoid ) ->
	Key = format_key_online( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加需要连接的所有服务器(散列类型数据)
%% 返回值 : {ok,success}
%% 使用管道添加,以减少与服务器的通信次数
%% ConnectDevTypeList : 需要连接的服务器的类型列表
-spec add_logic_server_connections(pid(),string(),list()) -> {ok,success}.
add_logic_server_connections( RedisClient,DevMoid,ConnectDevTypeList ) ->
	Key = format_key_connection( DevMoid ),
	Fun = fun( ConnectDevType, Acc ) ->
			[["HSET",Key,ConnectDevType,""]|Acc]
	      end,
		  
	Pipeline = lists:foldl( Fun,[],ConnectDevTypeList ),
	eredis:qp(RedisClient,Pipeline),
	{ok,success}.

%% 删除服务器的连接情况(散列类型数据)	
%% 返回值 : {ok,<<"1">>} 
-spec del_logic_server_connections(pid(),string()) -> {ok,binary()}.
del_logic_server_connections( RedisClient,DevMoid ) ->
	Key = format_key_connection( DevMoid ),
	eredis:q(RedisClient,["DEL",Key]).

%% 添加一个需要连接的服务器(散列类型数据)
%% 返回值 : {ok,<<"1">>}
-spec add_logic_server_connection(pid(),string(),string()) -> {ok,binary()}.
add_logic_server_connection( RedisClient,DevMoid,ConnectDevType ) ->
	Key = format_key_connection( DevMoid ),
	eredis:q(RedisClient,["HSET",Key,ConnectDevType,""]).
	
%% 删除一个需要连接的服务器(散列类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_logic_server_connection(pid(),string(),string()) -> {ok,binary()}.
del_logic_server_connection( RedisClient,DevMoid,ConnectDevType ) ->
	Key = format_key_connection( DevMoid ),
	eredis:q(RedisClient,["HDEL",Key,ConnectDevType]).
	
%% 更新多个服务器的连接状态(散列类型数据)
%% 返回值 : {ok,success} 
%% 使用管道添加,以减少与服务器的通信次数
%% ConnectedDevTypeList : 当前已连接的服务器类型列表,列表里面每项的内容是[DevType,IP]
-spec update_logic_server_connections(pid(),string(),list()) -> {ok,success}.
update_logic_server_connections( RedisClient,DevMoid,ConnectedDevTypeList ) ->
	Key = format_key_connection( DevMoid ),
	
	%% 先把所有需要连接的服务器IP都还原成空
	{ok,ServerList} = eredis:q(RedisClient,["HKEYS",Key]),
	Fun1 = fun( DevType, Acc ) ->
			[["HSET",Key,DevType,""]|Acc]
	      end,  
	Pipeline1 = lists:foldl( Fun1,[],ServerList ),
	eredis:qp(RedisClient,Pipeline1),
	
	%% 再更新列表里面的已连接上的服务器的IP
	Fun2 = fun( [DevType,IP], Acc ) ->
			[["HSET",Key,DevType,IP]|Acc]
	      end,	  
	Pipeline2 = lists:foldl( Fun2,[],ConnectedDevTypeList ),
	eredis:qp(RedisClient,Pipeline2),
	
	{ok,success}.
	
%% 更新一个服务器的连接状态(散列类型数据)
%% 返回值 : {ok,<<"1">>}
-spec update_logic_server_connection(pid(),string(),string(),string()) -> {ok,binary()}.
update_logic_server_connection( RedisClient,DevMoid,ConnectDevType,IP ) ->
	Key = format_key_connection( DevMoid ),
	eredis:q(RedisClient,["HSET",Key,ConnectDevType,IP]).
	
%% 根据moid获取指定逻辑服务器的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok, {Moid,Guid,PServerMoid,DomainMoid,Name,Type,IP}}
-spec get_logic_server_info_by_moid(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary(),binary(),binary(),binary()}.
get_logic_server_info_by_moid( RedisClient, DevMoid ) ->
	KeyInfo = format_key_moid_info( DevMoid ),
	case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3}
			            ,{Key4,Value4},{Key5,Value5},{Key6,Value6}],
			{<<"moid">>,Moid}                 = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}                 = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"p_server_moid">>,PServerMoid} = lists:keyfind(<<"p_server_moid">>,1,ValueList),
			{<<"domain_moid">>,DomainMoid}    = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}                 = lists:keyfind(<<"name">>,1,ValueList),
			{<<"type">>,Type}                 = lists:keyfind(<<"type">>,1,ValueList),
			
			{ok, {Moid,Guid,PServerMoid,DomainMoid,Name,<<"">>,Type}};

		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3}
			            ,{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7}],
			{<<"moid">>,Moid}                 = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}                 = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"p_server_moid">>,PServerMoid} = lists:keyfind(<<"p_server_moid">>,1,ValueList),
			{<<"domain_moid">>,DomainMoid}    = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}                 = lists:keyfind(<<"name">>,1,ValueList),
			{<<"ip">>,IP}                     = lists:keyfind(<<"ip">>,1,ValueList),
			{<<"type">>,Type}                 = lists:keyfind(<<"type">>,1,ValueList),
			
			{ok, {Moid,Guid,PServerMoid,DomainMoid,Name,IP,Type}}
	end.

%% 根据guid获取指定逻辑服务器的入网信息(散列类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok, {Moid,Guid,PhyServerMoid,PlatformDomainMoid,Name,Type,IP}}
-spec get_logic_server_info_by_guid(pid(),string()|binary()) -> {error,binary()}|{binary(),binary(),binary(),binary(),binary(),binary(),binary()}.
get_logic_server_info_by_guid( RedisClient, DevGuid ) ->
	KeyInfo = format_key_guid_info( DevGuid ),
	case eredis:q(RedisClient,["HGETALL",KeyInfo]) of
		{ok,[]} ->
			{error,<<"Key Error">>};
			
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3},{Key4,Value4},{Key5,Value5}],
			{<<"moid">>,Moid}                      = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}                      = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"domain_moid">>,PlatformDomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}                      = lists:keyfind(<<"name">>,1,ValueList),
			{<<"type">>,Type}                      = lists:keyfind(<<"type">>,1,ValueList),
			
			{ok, {Moid,Guid,<<"">>,PlatformDomainMoid,Name,<<"">>,Type}};

		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3}
			            ,{Key4,Value4},{Key5,Value5},{Key6,Value6}],
			{<<"moid">>,Moid}                      = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}                      = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"p_server_moid">>,PhyServerMoid}    = lists:keyfind(<<"p_server_moid">>,1,ValueList),
			{<<"domain_moid">>,PlatformDomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}                      = lists:keyfind(<<"name">>,1,ValueList),
			{<<"type">>,Type}                      = lists:keyfind(<<"type">>,1,ValueList),
			
			{ok, {Moid,Guid,PhyServerMoid,PlatformDomainMoid,Name,<<"">>,Type}};
			
		{ok,[Key1,Value1,Key2,Value2,Key3,Value3,Key4,Value4,Key5,Value5,Key6,Value6,Key7,Value7]} ->
			
			ValueList = [{Key1,Value1},{Key2,Value2},{Key3,Value3}
			            ,{Key4,Value4},{Key5,Value5},{Key6,Value6},{Key7,Value7}],
			{<<"moid">>,Moid}                      = lists:keyfind(<<"moid">>,1,ValueList),
			{<<"guid">>,Guid}                      = lists:keyfind(<<"guid">>,1,ValueList),
			{<<"p_server_moid">>,PhyServerMoid}    = lists:keyfind(<<"p_server_moid">>,1,ValueList),
			{<<"domain_moid">>,PlatformDomainMoid} = lists:keyfind(<<"domain_moid">>,1,ValueList),
			{<<"name">>,Name}                      = lists:keyfind(<<"name">>,1,ValueList),
			{<<"ip">>,IP}                          = lists:keyfind(<<"ip">>,1,ValueList),
			{<<"type">>,Type}                      = lists:keyfind(<<"type">>,1,ValueList),
			
			{ok, {Moid,Guid,PhyServerMoid,PlatformDomainMoid,Name,IP,Type}}
	end.
	
%% 获取指定逻辑服务器的在线状态(字符串类型数据)
%% 返回值 : {error,<<"Key Error">>}|{ok,<<"online">>} 
-spec get_logic_server_online(pid(),string()|binary()) -> {error,binary()}|{ok,binary()}.
get_logic_server_online( RedisClient, DevMoid ) ->
	Key = format_key_online( DevMoid ),
	case eredis:q(RedisClient,["GET",Key]) of
		{ok,undefined} ->
			{error,<<"Key Error">>};
		{ok,Value} ->
			{ok,Value}
	end.

%% 为逻辑服务器添加一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>}
-spec add_logical_server_warning(pid(),string(),integer()) -> {ok,binary()}.	
add_logical_server_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SADD",Key,WarningCode]).

%% 为逻辑服务器删除一个告警码(集合类型数据)
%% 返回值 : {ok,<<"1">>} 
-spec del_logical_server_warning(pid(),string(),integer()) -> {ok,binary()}.	
del_logical_server_warning( RedisClient,DevMoid,WarningCode ) ->
	Key = format_key_warning( DevMoid ),
	eredis:q(RedisClient,["SREM",Key,WarningCode]).


%% 获取与指定服务器有连接的服务器列表(散列类型数据)
%% 返回值 : [Key1,Value1,Key2,Value2,...]
-spec get_logic_server_connections(pid(),string()|binary()) -> list().
get_logic_server_connections( RedisClient,DevMoid ) ->
	Key = format_key_connection(DevMoid),
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

format_key_xmpp_online(DomainMoid) when is_list(DomainMoid)   -> "domain:" ++ DomainMoid ++ ":xmpp_online";
format_key_xmpp_online(DomainMoid) when is_binary(DomainMoid) -> "domain:" ++ binary_to_list(DomainMoid) ++ ":xmpp_online".

format_key_pas_online(DomainMoid,DevMoid) when is_list(DomainMoid) andalso is_list(DevMoid) -> 
	"domain:" ++ DomainMoid ++ ":pas:" ++ DevMoid;
format_key_pas_online(DomainMoid,DevMoid) when is_binary(DomainMoid) andalso is_binary(DevMoid)-> 
	"domain:" ++ binary_to_list(DomainMoid) ++ ":pas:" ++ binary_to_list(DevMoid).

format_key_pas(DomainMoid) when is_list(DomainMoid)   -> "domain:" ++ DomainMoid ++ ":pas";
format_key_pas(DomainMoid) when is_binary(DomainMoid) -> "domain:" ++ binary_to_list(DomainMoid) ++ ":pas".

format_key_logic_server(DevMoid) when is_list(DevMoid)   -> "p_server:" ++ DevMoid ++ ":l_server";
format_key_logic_server(DevMoid) when is_binary(DevMoid) -> "p_server:" ++ binary_to_list(DevMoid) ++ ":l_server".

format_key_moid_info(DevMoid) when is_list(DevMoid)   -> "l_server:" ++ DevMoid ++ ":info";
format_key_moid_info(DevMoid) when is_binary(DevMoid) -> "l_server:" ++ binary_to_list(DevMoid) ++ ":info".

format_key_guid_info(DevGuid) when is_list(DevGuid)   -> "l_server:" ++ DevGuid ++ ":info";
format_key_guid_info(DevGuid) when is_binary(DevGuid) -> "l_server:" ++ binary_to_list(DevGuid) ++ ":info".

format_key_online(DevMoid) when is_list(DevMoid)   -> "l_server:" ++ DevMoid ++ ":online";
format_key_online(DevMoid) when is_binary(DevMoid) -> "l_server:" ++ binary_to_list(DevMoid) ++ ":online".

format_key_warning(DevMoid) when is_list(DevMoid)   -> "l_server:" ++ DevMoid ++ ":warning";
format_key_warning(DevMoid) when is_binary(DevMoid) -> "l_server:" ++ binary_to_list(DevMoid) ++ ":warning".

format_key_connection(DevMoid) when is_list(DevMoid)   -> "l_server:" ++ DevMoid ++ ":connection";
format_key_connection(DevMoid) when is_binary(DevMoid) -> "l_server:" ++ binary_to_list(DevMoid) ++ ":connection".