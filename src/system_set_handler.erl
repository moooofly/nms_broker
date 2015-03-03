-module(system_set_handler).

-export([update_terminal_cpu_limit/3,
		 get_terminal_cpu_limit_redis/1,   %% add
		 get_terminal_cpu_limit_mysql/1,   %% add
		 get_terminal_cpu_limit/2,

		 update_terminal_memory_limit/3,
		 get_terminal_memory_limit_redis/1,   %% add
		 get_terminal_memory_limit_mysql/1,   %% add
		 get_terminal_memory_limit/2,

		 update_server_cpu_limit/3,
		 get_server_cpu_limit_redis/1,   %% add
		 get_server_cpu_limit_mysql/1,   %% add
		 get_server_cpu_limit/2,

		 update_server_mem_limit/3,      %% updated
		 get_server_mem_limit_redis/1,   %% add
		 get_server_mem_limit_mysql/1,   %% add
		 get_server_mem_limit/2,         %% updated

		 update_server_disk_limit/3,
		 get_server_disk_limit_redis/1,   %% add
		 get_server_disk_limit_mysql/1,   %% add
		 get_server_disk_limit/2,

		 update_server_net_limit/3,
		 get_server_net_limit_redis/1,   %% add
		 get_server_net_limit_mysql/1,   %% add
		 get_server_net_limit/2,

		 update_server_pas_limit/3,
		 get_server_pas_limit/2,
		 update_server_callpair_limit/3,
		 get_server_callpair_limit/2,
		 update_server_upu_limit/3,
		 get_server_upu_limit/2,
		 update_server_nms_limit/3,
		 get_server_nms_limit/2,
		 get_all_limit_info/2]).
		 
%% 更新终端CPU阈值
%% redis表格为字符串类型数据
-spec update_terminal_cpu_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_terminal_cpu_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","terminal_cpu_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES('"++integer_to_list(Limit)++"',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET t_cpu='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.

%% 从 Redis 获取终端 CPU 阈值
%% redis 表格为字符串类型数据
-spec get_terminal_cpu_limit_redis(pid()) -> binary()|undefined.
get_terminal_cpu_limit_redis(RedisClient) ->
	%% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
	Result = eredis:q(RedisClient,["GET","terminal_cpu_limit"]),
	Result.

%% 从 MySQL 获取终端 CPU 阈值
-spec get_terminal_cpu_limit_mysql(atom()) -> {ok,binary()}|{error,string()}.
get_terminal_cpu_limit_mysql(PoolId) ->
	SQL = "SELECT t_cpu FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Row]),
	[[Value]] = Row,
	Value.
	
%% 获取终端CPU阈值
%% redis表格为字符串类型数据
-spec get_terminal_cpu_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_terminal_cpu_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","terminal_cpu_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT t_cpu FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 更新终端内存阈值
%% redis表格为字符串类型数据
-spec update_terminal_memory_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_terminal_memory_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","terminal_memory_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,'"++integer_to_list(Limit)++"',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET t_memory='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.

%% 从 Redis 获取终端 MEM 阈值
%% redis 表格为字符串类型数据
-spec get_terminal_memory_limit_redis(pid()) -> binary()|undefined.
get_terminal_memory_limit_redis(RedisClient) ->
	%% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
	Result = eredis:q(RedisClient,["GET","terminal_memory_limit"]),
	Result.

%% 从 MySQL 获取终端 MEM 阈值
-spec get_terminal_memory_limit_mysql(atom()) -> {ok,binary()}|{error,string()}.
get_terminal_memory_limit_mysql(PoolId) ->
	SQL = "SELECT t_memory FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Row]),
	[[Value]] = Row,
	Value.

%% 获取终端CPU阈值
%% redis表格为字符串类型数据
-spec get_terminal_memory_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_terminal_memory_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","terminal_memory_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT t_memory FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.

%% 更新服务器CPU阈值
%% redis表格为字符串类型数据
-spec update_server_cpu_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_cpu_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_cpu_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,'"++integer_to_list(Limit)++"',NULL,NULL,NULL,NULL,NULL,NULL,NULL);",
			lager:info("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_cpu='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.

%% 从 Redis 的 STRING 表中获取服务器 Cpu 阈值
%% 返回值 {ok, return_value()} | {error, no_connection}
-spec get_server_cpu_limit_redis(pid()) -> {ok, binary() | undefined} | {error, no_connection}.
get_server_cpu_limit_redis(RedisClient) ->
	%% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
	Result = eredis:q(RedisClient,["GET","server_cpu_limit"]),
	Result.

%% 从 MySQL 获取服务器 Cpu 阈值
-spec get_server_cpu_limit_mysql(atom()) -> {ok,binary()}|{error,string()}.
get_server_cpu_limit_mysql(PoolId) ->
	SQL = "SELECT s_cpu FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Value,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:info("The Value is : ~p~n",[Value]),
	Value.

%% 获取服务器 CPU 阈值
-spec get_server_cpu_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_cpu_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_cpu_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_cpu FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.

%% 更新服务器 Mem 阈值
%% redis表格为字符串类型数据
-spec update_server_mem_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_mem_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_memory_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,'"++integer_to_list(Limit)++"',NULL,NULL,NULL,NULL,NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_memory='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.

%% 从 Redis 的 STRING 表中获取服务器 Mem 阈值
%% 返回值 {ok, return_value()} | {error, no_connection}
-spec get_server_mem_limit_redis(pid()) -> {ok, binary() | undefined} | {error, no_connection}.
get_server_mem_limit_redis(RedisClient) ->
	eredis:q(RedisClient,["GET","server_memory_limit"]).

%% 从 MySQL 获取服务器 Mem 阈值
-spec get_server_mem_limit_mysql(atom()) -> {ok,binary()}|{error,string()}.
get_server_mem_limit_mysql(PoolId) ->
	SQL = "SELECT s_memory FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Value,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:info("The Value is : ~p~n",[Value]),
	Value.

%% 获取服务器内存阈值
-spec get_server_mem_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_mem_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_memory_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_memory FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.

%% 更新服务器磁盘阈值
%% redis表格为字符串类型数据
-spec update_server_disk_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_disk_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_disk_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,NULL,'"++integer_to_list(Limit)++"',NULL,NULL,NULL,NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_disk='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.

%% 从 Redis 的 STRING 表中获取服务器 Disk 阈值
-spec get_server_disk_limit_redis(pid()) -> binary()|undefined.
get_server_disk_limit_redis(RedisClient) ->
	%% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
	Result = eredis:q(RedisClient,["GET","server_disk_limit"]),
	Result.

%% 从 MySQL 获取服务器 Disk 阈值
-spec get_server_disk_limit_mysql(atom()) -> {ok,binary()}|{error,string()}.
get_server_disk_limit_mysql(PoolId) ->
	SQL = "SELECT s_disk FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Value,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Value]),
	Value.

%% 获取服务器 Disk 阈值
-spec get_server_disk_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_disk_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_disk_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_disk FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 更新服务器网口吞吐量阈值
%% redis表格为字符串类型数据
-spec update_server_net_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_net_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_port_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,NULL,NULL,'"++integer_to_list(Limit)++"',NULL,NULL,NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_port='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.


%% 从 Redis 的 STRING 表中获取服务器 网口吞吐量 阈值
-spec get_server_net_limit_redis(pid()) -> binary()|undefined.
get_server_net_limit_redis(RedisClient) ->
	%% Result = {ok, return_value()} | {error, Reason::binary() | no_connection}
	Result = eredis:q(RedisClient,["GET","server_port_limit"]),
	Result.

%% 从 MySQL 获取服务器 网口吞吐量 阈值
-spec get_server_net_limit_mysql(atom()) -> {ok,binary()}|{error,string()}.
get_server_net_limit_mysql(PoolId) ->
	SQL = "SELECT s_port FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Value,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Value]),
	Value.
	
%% 获取服务器 网口吞吐量 阈值
%% redis表格为字符串类型数据
-spec get_server_net_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_net_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_port_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_port FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 更新服务器PAS接入数量阈值
%% redis表格为字符串类型数据
-spec update_server_pas_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_pas_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_pas_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,NULL,NULL,NULL,'"++integer_to_list(Limit)++"',NULL,NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_pas='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 获取服务器PAS接入数量阈值
%% redis表格为字符串类型数据
-spec get_server_pas_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_pas_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_pas_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_pas FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 更新服务器MTS呼叫对阈值
%% redis表格为字符串类型数据
-spec update_server_callpair_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_callpair_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_callpair_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,NULL,NULL,NULL,NULL,'"++integer_to_list(Limit)++"',NULL,NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_callpair='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 获取服务器MTS呼叫对阈值
%% redis表格为字符串类型数据
-spec get_server_callpair_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_callpair_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_callpair_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_callpair FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 更新服务器UPU接入数量阈值
%% redis表格为字符串类型数据
-spec update_server_upu_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_upu_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_upu_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'"++integer_to_list(Limit)++"',NULL);",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_upu='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 获取服务器UPU接入数量阈值
%% redis表格为字符串类型数据
-spec get_server_upu_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_upu_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_upu_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_upu FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 更新服务器NMS接入数量阈值
%% redis表格为字符串类型数据
-spec update_server_nms_limit(pid(),integer(),atom()) -> {ok,success}|{error,string()}.
update_server_nms_limit( RedisClient,Limit,PoolId ) ->
	%% reidis里面先存一份
	eredis:q(RedisClient,["SET","server_nms_limit",Limit]),
	
	SQL = "SELECT * FROM resource_limit;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO resource_limit VALUES(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'"++integer_to_list(Limit)++"');",
			io:format("The SQL1 is : ~p~n",[SQL1]),
	        Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			SQL1 = "UPDATE resource_limit SET s_nms='"++integer_to_list(Limit)++"';",
			io:format("The SQL1 is : ~p~n",[SQL1]),
			Result = emysql:execute(PoolId,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 获取服务器NMS接入数量阈值
%% redis表格为字符串类型数据
-spec get_server_nms_limit(pid(),atom()) -> {ok,binary()}|{error,string()}.
get_server_nms_limit(RedisClient,PoolId) ->
	{ok,Value} = eredis:q(RedisClient,["GET","server_nms_limit"]),
	case Value of
		undefined ->
			SQL = "SELECT s_nms FROM resource_limit;",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Row,_} = emysql:execute(PoolId,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Row]),
			[[Value]] = Row,
			Value;
		_ ->
			Value
	end.
	
%% 获取所有的阈值信息
%% redis表格为字符串类型数据
-spec get_all_limit_info(pid(),atom()) -> {string(),string(),string(),string(),string(),string(),string(),string(),string(),string()}.
get_all_limit_info(RedisClient,PoolId) ->
	T_CPU = get_terminal_cpu_limit(RedisClient,PoolId),
	T_Memory = get_terminal_memory_limit(RedisClient,PoolId),
	S_CPU = get_server_cpu_limit(RedisClient,PoolId),
	S_Memory = get_server_mem_limit(RedisClient,PoolId),
	S_Disk = get_server_disk_limit(RedisClient,PoolId),
	S_Port = get_server_net_limit(RedisClient,PoolId),
	S_Pas = get_server_pas_limit(RedisClient,PoolId),
	S_CallPair = get_server_callpair_limit(RedisClient,PoolId),
	S_NMS = get_server_nms_limit(RedisClient,PoolId),
	S_UPU = get_server_upu_limit(RedisClient,PoolId),
	{T_CPU,T_Memory,S_CPU,S_Memory,S_Disk,S_Port,S_Pas,S_CallPair,S_NMS,S_UPU}.
