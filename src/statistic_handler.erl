-module(statistic_handler).

-export([update_terminal_online_count/4,
         get_terminal_online_count/3,
		 update_web_resource_count/4,
         get_web_resource_count/3,
         add_terminal_online_statistic/7,
         add_terminal_version_statistic/7,
         add_cpu_statistic/6,       %% updated
		 add_memory_statistic/5,    %% updated
		 add_disk_statistic/5,      %% updated
		 add_net_statistic/7,       %% updated
		 add_web_resource_statistic/7,
		 add_media_resource_statistic/4,
		 add_warning_repair_statistic/6,   %% updated
         get_terminal_online_statistic/3,
		 get_terminal_version_statistic/2,
         get_cpu_statictic/4,
		 get_memory_statictic/4,
		 get_disk_statictic/4,
		 get_netcard_statictic/4,
		 get_web_resource_statistic/3,
		 get_media_resource_statistic/3,
		 get_warning_repair_statistic/3,
		 get_warning_repair_statistic/4,
		 get_warning_repair_statistic/5]).
	
%% 更新终端在线数量(字符串类型数据)
%% 返回值: {ok,<<"1">>} | {error,<<"Invalid type">>}
-spec update_terminal_online_count(pid(),string(),atom(),integer()) -> {ok,binary()}|{error,binary()}.
update_terminal_online_count(RedisClient,DomainMoid,Type,Count) ->
	case Type of
		xmpp ->
			Key = "domain:"++DomainMoid++":xmpponlie",
			eredis:q(RedisClient,["SET",Key,Count]);
		sip ->
			Key = "domain:"++DomainMoid++":siponlie",
			eredis:q(RedisClient,["SET",Key,Count]);
		tip ->
			Key = "domain:"++DomainMoid++":tiponlie",
			eredis:q(RedisClient,["SET",Key,Count]);
		monitor ->
			Key = "domain:"++DomainMoid++":monitoronlie",
			eredis:q(RedisClient,["SET",Key,Count]);
		h323 ->
			Key = "domain:"++DomainMoid++":h323onlie",
			eredis:q(RedisClient,["SET",Key,Count]);
		_ ->
			{error,<<"Invalid type">>}
	end.

%% 获取终端在线数量(字符串类型数据)
-spec get_terminal_online_count(pid(),string(),atom()) -> integer()|{error,binary()}.
get_terminal_online_count(RedisClient,DomainMoid,Type) ->
	case Type of
		xmpp ->
			Key = "domain:"++DomainMoid++":xmpponlie",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		sip ->
			Key = "domain:"++DomainMoid++":siponlie",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		tip ->
			Key = "domain:"++DomainMoid++":tiponlie",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		monitor ->
			Key = "domain:"++DomainMoid++":monitoronlie",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		h323 ->
			Key = "domain:"++DomainMoid++":h323onlie",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		_ ->
			{error,<<"Invalid type">>}
	end.
	
%% 更新web并发数量(字符串类型数量)
%% 返回值 : {ok,<<"1">>} | {error,<<"Invalid type">>}
-spec update_web_resource_count(pid(),string(),atom(),integer()) -> {ok,binary()}|{error,binary()}.
update_web_resource_count(RedisClient,DomainMoid,Type,Count) ->
	case Type of
		account_center ->
			Key = "domain:"++DomainMoid++":web_resource:account_center",
			eredis:q(RedisClient,["SET",Key,Count]);
		meeting_center ->
			Key = "domain:"++DomainMoid++"::web_resource:meeting_center",
			eredis:q(RedisClient,["SET",Key,Count]);
		record_service ->
			Key = "domain:"++DomainMoid++"::web_resource:record_service",
			eredis:q(RedisClient,["SET",Key,Count]);
		data_meeting ->
			Key = "domain:"++DomainMoid++"::web_resource:data_meeting",
			eredis:q(RedisClient,["SET",Key,Count]);
		communication ->
			Key = "domain:"++DomainMoid++"::web_resource:communication",
			eredis:q(RedisClient,["SET",Key,Count]);
		_ ->
			{error,<<"Invalid type">>}
	end.

%% 获取web并发数量(字符串类型数据)
-spec get_web_resource_count(pid(),string(),atom()) -> integer()|{error,binary()}.
get_web_resource_count(RedisClient,DomainMoid,Type) ->
	case Type of
		account_center ->
			Key = "domain:"++DomainMoid++":web_resource:account_center",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		meeting_center ->
			Key = "domain:"++DomainMoid++"::web_resource:meeting_center",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		record_service ->
			Key = "domain:"++DomainMoid++"::web_resource:record_service",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		data_meeting ->
			Key = "domain:"++DomainMoid++"::web_resource:data_meeting",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		communication ->
			Key = "domain:"++DomainMoid++"::web_resource:communication",
			case eredis:q(RedisClient,["GET",Key]) of
				{ok,undefined} ->
					0;
				{ok,Value} ->
					list_to_integer(binary_to_list(Value))
			end;
		_ ->
			{error,<<"Invalid type">>}
	end.
		 
%% 添加一个终端在线统计数据
-spec add_terminal_online_statistic(string(),integer(),integer(),integer(),integer(),integer(),string()) -> {ok,success}|{error,string()}.
add_terminal_online_statistic(DomainMoid,XmppOnline,SipOnline,TipOnline,MonitorOnline,H323Online,StatisticTime) ->
	SQL = "INSERT INTO terminal_online_statistic VALUES(NULL,'"++DomainMoid++"',"++integer_to_list(XmppOnline)++","++integer_to_list(SipOnline)++","
	      ++integer_to_list(TipOnline)++","++integer_to_list(MonitorOnline)++","++integer_to_list(H323Online)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 添加一个终端版本统计数据
-spec add_terminal_version_statistic(string(),string(),string(),integer(),string(),string(),string()) -> {ok,success}|{error,string()}.
add_terminal_version_statistic( DomainMoid,DevMoid,E164,Type,Oem,Version,Recommend ) ->
    %% 先确定指定账号的指定终端类型的版本号是否已经存在数据库
	%% 如果已经存在,则只更新版本号,如果不存在,则插入一行数据
	SQL1 = "SELECT * from terminal_version_statistic WHERE domain_moid = '"++DomainMoid
	       ++"' && device_moid = '"++DevMoid++"' && type = '"++integer_to_list(Type)++"';",
	io:format("The SQL1 is : ~p~n",[SQL1]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Rows of
		[] ->
			SQL2 = "INSERT INTO terminal_version_statistic VALUES(NULL,'"++DomainMoid
			      ++"','"++DevMoid++"','"++E164++"','"++integer_to_list(Type)++"','"++Oem++"','"++Version++"','"++Recommend++"');",
			io:format("The SQL2 is : ~p~n",[SQL2]),
			Result2 = emysql:execute(nms_cache_pool,list_to_binary(SQL2)),
			io:format("The Result2 is : ~p~n",[Result2]),
			case Result2 of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL3 = "UPDATE terminal_version_statistic SET version = '"++Version++"' oem = '"++Oem++"' recommend='"++Recommend
			++"' WHERE domain_moid='"++DomainMoid++"' && device_moid = '"++DevMoid++"' && type = '"++integer_to_list(Type)++"';",
			io:format("The SQL3 is : ~p~n",[SQL3]),
			Result3 = emysql:execute(nms_cache_pool,list_to_binary(SQL3)),
			io:format("The Result3 is : ~p~n",[Result3]),
			case Result3 of
				{error_packet,_,_,_,Msg2} ->
					{error,Msg2};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 添加一个物理设备的CPU统计数据
-spec add_cpu_statistic(atom(),string(),string(),integer(),integer(),string()) -> {ok,success}|{error,string()}.
add_cpu_statistic(PoolId,DomainMoid,DevMoid,CpuID,Cpu,StatisticTime) ->
	SQL = "INSERT INTO cpu_statistic VALUES(NULL,'"++DomainMoid++"','"++DevMoid++"',"++integer_to_list(CpuID)++","++integer_to_list(Cpu)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 添加一个物理设备的MEM存统计数据
-spec add_memory_statistic(atom(), string(),string(),integer(),string()) -> {ok,success}|{error,string()}.
add_memory_statistic(PoolId,DomainMoid,DevMoid,Memory,StatisticTime) ->
	SQL = "INSERT INTO memory_statistic VALUES(NULL,'"++DomainMoid++"','"++DevMoid++"',"++integer_to_list(Memory)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 添加一个物理设备的disk统计数据
-spec add_disk_statistic(atom(),string(),string(),integer(),string()) -> {ok,success}|{error,string()}.	
add_disk_statistic(PoolId,DomainMoid,DevMoid,Disk,StatisticTime) ->
	SQL = "INSERT INTO disk_statistic VALUES(NULL,'"++DomainMoid++"','"++DevMoid++"',"++integer_to_list(Disk)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%% 添加一个物理设备的网卡统计数据
-spec add_net_statistic(atom(),string(),string(),integer(),integer(),integer(),string()) -> {ok,success}|{error,string()}.
add_net_statistic(PoolId,DomainMoid,DevMoid,CardID,PortIn,PortOut,StatisticTime) ->
	SQL = "INSERT INTO netcard_statistic VALUES(NULL,'"++DomainMoid++"','"++DevMoid++"',"
	      ++integer_to_list(CardID)++","++integer_to_list(PortIn)++","++integer_to_list(PortOut)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%% 添加一个web并发统计数据
-spec add_web_resource_statistic(string(),integer(),integer(),integer(),integer(),integer(),string()) -> {ok,success}|{error,string()}.	
add_web_resource_statistic(DomainMoid,AcountCenter,MeetingCenter,RecordService,DataMeeting,Communication,StatisticTime) ->
	SQL = "INSERT INTO web_resource_statistic VALUES(NULL,'"++DomainMoid++"',"++integer_to_list(AcountCenter)++","++integer_to_list(MeetingCenter)++","
	      ++integer_to_list(RecordService)++","++integer_to_list(DataMeeting)++","++integer_to_list(Communication)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%% 添加一个媒体资源占用统计数据
-spec add_media_resource_statistic(string(),integer(),integer(),string()) -> {ok,success}|{error,string()}.	
add_media_resource_statistic(DomainMoid,TMeeting,PMeeting,StatisticTime) ->
	SQL = "INSERT INTO media_resource_statistic VALUES(NULL,'"++DomainMoid++"',"++integer_to_list(TMeeting)++","
	      ++integer_to_list(PMeeting)++",'"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 添加一个告警与修复统计数据
-spec add_warning_repair_statistic(atom(),string(),string(),integer(),string(),string()) -> {ok,success}|{error,string()}.	
add_warning_repair_statistic(PoolId,DomainMoid,DevMoid,WarningCode,Status,StatisticTime) ->
	SQL = "INSERT INTO warning_repair_statistic VALUES(NULL,'"++DomainMoid++"','"++DevMoid++"',"
	      ++integer_to_list(WarningCode)++",'"++Status++"','"++StatisticTime++"');",
	io:format("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	io:format("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
		
%% 获取指定域的终端在线统计数据
-spec get_terminal_online_statistic(string(),string(),string()) -> list().
get_terminal_online_statistic( DomainMoid, StartTime, StopTime ) ->
	SQL = "SELECT * FROM terminal_online_statistic WHERE domain_moid = '"++DomainMoid++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定域的终端版本统计数据
-spec get_terminal_version_statistic(string(),string()) -> list().
get_terminal_version_statistic( DomainMoid,Oem ) ->
    case Oem of
		[] ->
			SQL = "SELECT * FROM terminal_version_statistic WHERE domain_moid = '"++DomainMoid++"';",
			io:format("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows;
		_ ->
			SQL = "SELECT * FROM terminal_version_statistic WHERE domain_moid = '"++DomainMoid++"' && oem = '"++Oem++"';",
			io:format("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows
	end.
	
%% 获取指定设备的cpu使用情况统计数据
-spec get_cpu_statictic(string(),string(),string(),string()) -> list().
get_cpu_statictic( DomainMoid,DevMoid,StartTime,StopTime ) ->
	SQL = "SELECT * FROM cpu_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++DevMoid
	      ++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定设备的内存使用情况统计数据
-spec get_memory_statictic(string(),string(),string(),string()) -> list().
get_memory_statictic( DomainMoid, DevMoid, StartTime, StopTime ) ->
	SQL = "SELECT * FROM memory_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++DevMoid
	      ++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定设备的磁盘使用情况统计数据
-spec get_disk_statictic(string(),string(),string(),string()) -> list().
get_disk_statictic( DomainMoid, DevMoid, StartTime, StopTime ) ->
	SQL = "SELECT * FROM disk_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++DevMoid
	      ++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定设备的网卡使用情况统计数据
-spec get_netcard_statictic(string(),string(),string(),string()) -> list().
get_netcard_statictic( DomainMoid, DevMoid, StartTime, StopTime ) ->
	SQL = "SELECT * FROM netcard_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++DevMoid
	      ++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定域的web并发统计数据
-spec get_web_resource_statistic(string(),string(),string()) -> list().
get_web_resource_statistic( DomainMoid, StartTime, StopTime ) ->
	SQL = "SELECT * FROM web_resource_statistic WHERE domain_moid = '"++DomainMoid++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定域的媒体资源占用情况统计数据
-spec get_media_resource_statistic(string(),string(),string()) -> list().
get_media_resource_statistic( DomainMoid, StartTime, StopTime ) ->
	SQL = "SELECT * FROM media_resource_statistic WHERE domain_moid = '"++DomainMoid++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.

%% 获取指定设备的告警与修复情况统计数据
-spec get_warning_repair_statistic(string(),string(),string()) -> list().
get_warning_repair_statistic( DomainMoid,StartTime,StopTime ) ->
	SQL = "SELECT * FROM warning_repair_statistic WHERE domain_moid = '"++DomainMoid
	++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定设备的告警与修复情况统计数据
-spec get_warning_repair_statistic(string(),string(),string(),string()) -> list().
get_warning_repair_statistic( DomainMoid,DevMoid,StartTime,StopTime ) ->
	SQL = "SELECT * FROM warning_repair_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++DevMoid
	++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定设备的告警与修复情况统计数据
-spec get_warning_repair_statistic(string(),string(),list(),string(),string()) -> list().
get_warning_repair_statistic( DomainMoid,DevMoid,LogicServerMoidList,StartTime,StopTime ) ->
	SQL1 = "SELECT * FROM warning_repair_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++DevMoid
	++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	io:format("The SQL1 is : ~p~n",[SQL1]),
	{result_packet,_,_,PhysicalStatistic,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	
	Fun = fun( Moid, Acc ) ->
			SQL2 = "SELECT * FROM warning_repair_statistic WHERE domain_moid = '"++DomainMoid++"' && device_moid = '"++Moid
			++"' && statistic_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The SQL2 is : ~p~n",[SQL2]),
			{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL2)),
			Rows ++ Acc
	      end,
		  
	LogicStatistic = lists:foldl( Fun, [], LogicServerMoidList ),
	
	PhysicalStatistic++LogicStatistic.
