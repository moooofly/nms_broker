-module(warning_handler).

-export([add_warning_code/6,
         add_sub_warning_code/3,
		 del_sub_warning_code/3,
		 add_warning_notify_email/3,
		 del_warning_notify_email/3,
		 add_warning_notify_phone/3,
		 del_warning_notify_phone/3,
		 add_warning_terminal_guid/3,
		 del_warning_terminal_guid/3,
		 add_warning_terminal_name/3,
		 del_warning_terminal_name/3,
		 add_warning_server_guid/3,
		 del_warning_server_guid/3,
		 add_warning_server_name/3,
		 del_warning_server_name/3,
		 get_all_warning_code/0,
		 get_warning_code_detail/2,
		 get_sub_warning_code/2,
		 get_warning_notify_email/2,
		 get_warning_notify_phone/2,
		 get_warning_terminal_guid/2,
		 get_warning_terminal_name/2,
		 get_warning_server_guid/2,
		 get_warning_server_name/2,
		 enable_warning_filter/3,
		 enable_warning_music/3,
		 enable_warning_email/3,
		 enable_warning_phone/3,
		 get_warning_set/2,
		 set_stop_all_warning/2,
		 get_stop_all_warning/1,
		 add_repaired_warning/9,      %% updated
		 add_unrepaired_warning/8,    %% updated
		 del_unrepaired_warning/4,    %% updated
		 get_sub_server_warning/3,
		 get_sub_server_warning/4,
		 get_sub_server_warning/5,
		 get_sub_server_warning/6,
		 get_sub_terminal_warning/3,
		 get_sub_terminal_warning/4,
		 get_sub_terminal_warning/5,
		 get_sub_terminal_warning/6,
		 get_all_server_warning/1,
		 get_all_server_warning/2,
		 get_all_server_warning/3,
		 get_all_server_warning/4,
		 get_all_terminal_warning/1,
		 get_all_terminal_warning/2,
		 get_all_terminal_warning/3,
		 get_all_terminal_warning/4,
		 get_device_warning_by_code/4,   %% add by me
		 get_device_warning/2,
		 get_device_warning/3,
		 get_device_warning/4,
		 get_device_warning/5]).
		 
%% 告警设备类型定义
-define(P_SERVER,0).
-define(L_SERVER,1).
-define(TERMINAL,2).
	
%%添加一条告警码信息
-spec add_warning_code(string(),integer(),string(),string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_code(Type,Code,Name,Level,Description,Suggestion) ->
	UType = binary_to_list(unicode:characters_to_binary(Type)),
	UName = binary_to_list(unicode:characters_to_binary(Name)),
	UDescription = binary_to_list(unicode:characters_to_binary(Description)),
	USuggestion = binary_to_list(unicode:characters_to_binary(Suggestion)),

	SQL = "INSERT INTO warning_code_utf8 VALUES(NULL,'"++UType++"',"++integer_to_list(Code)++",'"++UName++"','"
	                                        ++Level++"','"++UDescription++"','"++USuggestion++"');",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%%添加一条已修复的告警信息
-spec add_repaired_warning(atom(), string(),integer(),string(),integer(),string(),string(),string(),string()) -> {ok,success}|{error,string()}.
add_repaired_warning(PoolId, DevGuid,DevType,DomainGuid,Code,Level,Description,StartTime,ResolveTime) ->
    SQL = "INSERT INTO warning_repaired VALUES(NULL,'"++DevGuid++"',"++integer_to_list(DevType)++",'"++DomainGuid++"',"
	                                        ++integer_to_list(Code)++",'"++Level++"','"++Description++"','"++StartTime++"','"++ResolveTime++"');",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%%添加一条未修复的告警信息
-spec add_unrepaired_warning(atom(),string(),integer(),string(),integer(),string(),string(),string()) -> {ok,success}|{error,string()}.
add_unrepaired_warning(PoolId,DevGuid,DevType,DomainGuid,Code,Level,Description,StartTime) ->
    SQL = "INSERT INTO warning_unrepaired VALUES(NULL,'"++DevGuid++"',"++integer_to_list(DevType)++",'"++DomainGuid++"',"
	                                        ++integer_to_list(Code)++",'"++Level++"','"++Description++"','"++StartTime++"',NULL);",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%%删除一条未修复的告警信息
-spec del_unrepaired_warning(atom(), string(),string(),integer()) -> {ok,success}|{error,string()}.
del_unrepaired_warning(PoolId, DevGuid,DomainGuid,Code) ->
    SQL = "DELETE FROM warning_unrepaired WHERE device_moid='"++DevGuid++"' && domain_moid='"++DomainGuid++"' && code="++integer_to_list(Code)++";",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(PoolId,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 为用户添加一个订阅告警码
-spec add_sub_warning_code(string(),string(),integer()) -> {ok,success}|{error,string()}.
add_sub_warning_code(DomainGuid,UserID,SubCode) ->
    SQL1 = "SELECT * FROM sub_warning_code WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && sub_code="++integer_to_list(SubCode)++";",
	io:format("The SQL1 is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of
		[] ->
			SQL = "INSERT INTO sub_warning_code VALUES(NULL,'"++DomainGuid++"','"++UserID++"',"++integer_to_list(SubCode)++");",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			io:format("The user [~p] have already subscribed warning code [~p],nothing to do!~n",[UserID,SubCode]),
			{ok,success}
	end.
	
%% 为用户删除一个订阅告警码
-spec del_sub_warning_code(string(),string(),integer()) -> {ok,success}|{error,string()}.
del_sub_warning_code(DomainGuid,UserID,SubCode) ->
	SQL = "DELETE FROM sub_warning_code WHERE domain_id = '"++DomainGuid++"' && user_id='"++UserID++"' && sub_code = "++integer_to_list(SubCode)++";",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%% 为用户添加一个告警通知邮件
-spec add_warning_notify_email(string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_notify_email( DomainGuid,UserID,Email ) ->
	SQL1 = "SELECT * FROM warning_notify_email WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && email='"++Email++"';",
	io:format("The SQL1 is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of
		[] ->
			SQL = "INSERT INTO warning_notify_email VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Email++"');",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
		    io:format("The user [~p] have already had the notify email [~p],nothing to do!~n",[UserID,Email]),
			{ok,success}
	end.
	
%% 为用户删除一个告警通知邮件
-spec del_warning_notify_email(string(),string(),string()) -> {ok,success}|{error,string()}.
del_warning_notify_email( DomainGuid,UserID,Email ) ->
	SQL = "DELETE FROM warning_notify_email WHERE domain_id = '"++DomainGuid++"' && user_id='"++UserID++"' && email = '"++Email++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 为用户添加一个告警通知电话
-spec add_warning_notify_phone(string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_notify_phone( DomainGuid,UserID,Phone ) ->
	SQL1 = "SELECT * FROM warning_notify_phone WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && phone='"++Phone++"';",
	io:format("The SQL1 is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of 
		[] ->
			SQL = "INSERT INTO warning_notify_phone VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Phone++"');",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			io:format("The user [~p] have already had the notify phone [~p],nothing to do!~n",[UserID,Phone]),
			{ok,success}
	end.
	
%% 为用户删除一个告警通知电话
-spec del_warning_notify_phone(string(),string(),string()) -> {ok,success}|{error,string()}.
del_warning_notify_phone( DomainGuid,UserID,Phone ) ->
	SQL = "DELETE FROM warning_notify_phone WHERE domain_id = '"++DomainGuid++"' && user_id='"++UserID++"' && phone = '"++Phone++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 为用户添加一个告警过滤终端设备GUID
-spec add_warning_terminal_guid(string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_terminal_guid( DomainGuid,UserID,Guid ) ->
	SQL1 = "SELECT * FROM warning_terminal_guid WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && guid='"++Guid++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of
		[] ->
			SQL = "INSERT INTO warning_terminal_guid VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Guid++"');",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			io:format("The user [~p] have already had the terminal guid [~p],nothing to do!~n",[UserID,Guid]),
			{ok,success}
	end.
	
%% 为用户删除一个告警过滤终端设备GUID
-spec del_warning_terminal_guid(string(),string(),string()) -> {ok,success}|{error,string()}.
del_warning_terminal_guid( DomainGuid,UserID,Guid ) ->
	SQL = "DELETE FROM warning_terminal_guid WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && guid='"++Guid++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 为用户添加一个告警过滤终端设备名称
-spec add_warning_terminal_name(string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_terminal_name( DomainGuid,UserID,Name ) ->
	SQL1 = "SELECT * FROM warning_terminal_name WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && name='"++Name++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of
		[] ->
			SQL = "INSERT INTO warning_terminal_name VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Name++"');",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			io:format("The user [~p] have already had the terminal name [~p],nothing to do!~n",[UserID,Name]),
			{ok,success}
	end.
	
%% 为用户删除一个告警过滤终端设备名称
-spec del_warning_terminal_name(string(),string(),string()) -> {ok,success}|{error,string()}.
del_warning_terminal_name( DomainGuid,UserID,Name ) ->
	SQL = "DELETE FROM warning_terminal_name WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && name='"++Name++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.
	
%% 为用户添加一个告警过滤服务器设备GUID
-spec add_warning_server_guid(string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_server_guid( DomainGuid,UserID,Guid ) ->
	SQL1 = "SELECT * FROM warning_server_guid WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && guid='"++Guid++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of
		[] ->
			SQL = "INSERT INTO warning_server_guid VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Guid++"');",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			io:format("The user [~p] have already had the server guid [~p],nothing to do!~n",[UserID,Guid]),
			{ok,success}
	end.
	
%% 为用户删除一个告警过滤服务器设备GUID
-spec del_warning_server_guid(string(),string(),string()) -> {ok,success}|{error,string()}.
del_warning_server_guid( DomainGuid,UserID,Guid ) ->
	SQL = "DELETE FROM warning_server_guid WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && guid='"++Guid++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 为用户添加一个告警过滤服务器设备名称
-spec add_warning_server_name(string(),string(),string()) -> {ok,success}|{error,string()}.
add_warning_server_name( DomainGuid,UserID,Name ) ->
	SQL1 = "SELECT * FROM warning_server_name WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && name='"++Name++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,Row,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	case Row of
		[] ->
			SQL = "INSERT INTO warning_server_name VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Name++"');",
			lager:info("The SQL is : ~p~n",[SQL]),
			Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_ ->
			io:format("The user [~p] have already had the server name [~p],nothing to do!~n",[UserID,Name]),
			{ok,success}
	end.
	
%% 为用户删除一个告警过滤服务器设备名称
-spec del_warning_server_name(string(),string(),string()) -> {ok,success}|{error,string()}.
del_warning_server_name( DomainGuid,UserID,Name ) ->
	SQL = "DELETE FROM warning_server_name WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"' && name='"++Name++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	Result = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	lager:debug("The Result is : ~p~n",[Result]),
	case Result of
		{error_packet,_,_,_,Msg} ->
			{error,Msg};
		{ok_packet,_,_,_,_,_,_} ->
			{ok,success}
	end.

%% 获取所有的告警码
-spec get_all_warning_code() -> list().
get_all_warning_code() ->
	SQL = "SELECT * FROM warning_code;",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	Rows.
	
%% 获取指定告警码的详情
-spec get_warning_code_detail(atom(), integer()) -> {integer(),string(),integer(),string(),string(),string(),string()}.
get_warning_code_detail(PoolId, Code) ->
	SQL = "SELECT * FROM warning_code WHERE code="++integer_to_list(Code)++";",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Value,_}= emysql:execute(PoolId,list_to_binary(SQL)),
	lager:info("The Value is : ~ts~n",[Value]),
	Value.

%% 获取指定域下的所有订阅的服务器告警
%% 没有选择任何搜索条件
%% 返回用户所在域以及其子域下的所有服务器告警
-spec get_sub_server_warning(string(),string(),list()) -> list().
get_sub_server_warning( UserDomainGuid,UserID,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid
						 ++"' && device_type!="++integer_to_list(?TERMINAL)++" && code IN"++CodeStr2++";",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后在数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid
						 ++"' && device_type!="++integer_to_list(?TERMINAL)++" && code IN"++CodeStr2++";",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有订阅的服务器告警
%% 搜索条件选择了告警级别
%% 返回用户域以及其子域下的所有告警
-spec get_sub_server_warning(string(),string(),string(),list()) -> list().
get_sub_server_warning( UserDomainGuid,UserID,Level,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有订阅的服务器告警
%% 搜索条件选择了时间区间
%% 返回用户域以及其子域下的所有告警
-spec get_sub_server_warning(string(),string(),string(),string(),list()) -> list().
get_sub_server_warning( UserDomainGuid,UserID,StartTime,StopTime,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有订阅的服务器告警
%% 搜索条件选择了告警级别和时间区间
%% 返回用户域以及其子域下的所有告警
-spec get_sub_server_warning(string(),string(),string(),string(),string(),list()) -> list().
get_sub_server_warning( UserDomainGuid,UserID,Level,StartTime,StopTime,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有订阅的终端告警
%% 没有选择任何搜索条件
%% 返回用户所在域以及其子域下的所有终端告警
-spec get_sub_terminal_warning(string(),string(),list()) -> list().
get_sub_terminal_warning( UserDomainGuid,UserID,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003)"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后在从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid
						 ++"' && device_type="++integer_to_list(?TERMINAL)++" && code IN"++CodeStr2++";",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后在从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid
						 ++"' && device_type="++integer_to_list(?TERMINAL)++" && code IN"++CodeStr2++";",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.

%% 获取指定域下的所有订阅的终端告警
%% 搜索条件选择了告警级别
%% 返回用户域以及其子域下的所有终端告警
-spec get_sub_terminal_warning(string(),string(),string(),list()) -> list().
get_sub_terminal_warning( UserDomainGuid,UserID,Level,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有订阅的终端告警
%% 搜索条件选择了时间区间
%% 返回用户域以及其子域下的所有终端告警
-spec get_sub_terminal_warning(string(),string(),string(),string(),list()) -> list().
get_sub_terminal_warning( UserDomainGuid,UserID,StartTime,StopTime,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有订阅的终端告警
%% 搜索条件选择了告警级别和时间区间
%% 返回用户域以及其子域下的所有终端告警
-spec get_sub_terminal_warning(string(),string(),string(),string(),string(),list()) -> list().
get_sub_terminal_warning( UserDomainGuid,UserID,Level,StartTime,StopTime,DomainGuidList ) ->
	%% 先取出用户订阅的所有告警码
	CodeList = get_sub_warning_code(UserDomainGuid,UserID),
	%%下面的代码是把[1001,1002,1003]转换成"(1001,1002,1003,"
	CodeStr = lists:foldl(fun(Code,Acc) -> Acc++integer_to_list(Code)++"," end,"(",CodeList),
	%%下面的代码是把"(1001,1002,1003,"转换成"(1001,1002,1003"
	CodeStr1 = string:substr(CodeStr,1,string:len(CodeStr)-1),
	%%下面的代码是把"(1001,1002,1003"转换成"(1001,1002,1003)"
	CodeStr2 = CodeStr1 ++ ")",
	io:format("The CodeStr2 is : ~p~n",[CodeStr2]),
	
	%%然后从数据库查询已修复的订阅告警
	Fun1 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	%%然后从数据库查询未修复的订阅告警
	Fun2 = fun( DomainGuid,Acc ) ->
			WarningSQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			            ++" && code IN"++CodeStr2++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			io:format("The WarningSQL is : ~p~n",[WarningSQL]),
			{result_packet,_,_,WarningRows,_}= emysql:execute(nms_cache_pool,list_to_binary(WarningSQL)),
			WarningRows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.

%% 获取指定域下的所有服务器告警
-spec get_all_server_warning(list()) -> list().
get_all_server_warning( DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)++";",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)++";",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有服务器告警
-spec get_all_server_warning(string(),list()) -> list().
get_all_server_warning( Level,DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有服务器告警
-spec get_all_server_warning(string(),string(),list()) -> list().
get_all_server_warning( StartTime,StopTime,DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			      ++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			      ++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有服务器告警
-spec get_all_server_warning(string(),string(),string(),list()) -> list().
get_all_server_warning( Level,StartTime,StopTime,DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type!="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有终端告警
-spec get_all_terminal_warning(list()) -> list().
get_all_terminal_warning( DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)++";",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)++";",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有终端告警
-spec get_all_terminal_warning(string(),list()) -> list().
get_all_terminal_warning( Level,DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有终端告警
-spec get_all_terminal_warning(string(),string(),list()) -> list().
get_all_terminal_warning( StartTime,StopTime,DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			      ++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			      ++" && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.
	
%% 获取指定域下的所有终端告警
-spec get_all_terminal_warning(string(),string(),string(),list()) -> list().
get_all_terminal_warning( Level,StartTime,StopTime,DomainGuidList ) ->
	Fun1 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	RepairedWarningList = lists:foldl(Fun1,[],DomainGuidList),
	
	Fun2 = fun( DomainGuid,Acc ) ->
			SQL = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_type="++integer_to_list(?TERMINAL)
			      ++"&& level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
			lager:info("The SQL is : ~p~n",[SQL]),
			{result_packet,_,_,Rows,_}= emysql:execute(nms_cache_pool,list_to_binary(SQL)),
			Rows++Acc
	      end,
	UnRepairedWarningList = lists:foldl(Fun2,[],DomainGuidList),
	
	UnRepairedWarningList++RepairedWarningList.

%% 获取指定设备上的指定告警  
-spec get_device_warning_by_code(atom(),string(),string(),integer()) -> list().
get_device_warning_by_code(PoolId, DomainGuid, DevGuid, WarningCode) ->
	SQL = "SELECT * FROM warning_unrepaired WHERE domain_moid='"++DomainGuid++"' && device_moid='"++
				DevGuid++"' && code='"++integer_to_list(WarningCode)++"';",
%%	SQL = "SELECT * FROM warning_unrepaired WHERE domain_moid='1.1' && device_moid='1.1.1' && code='2002';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,UnRepairedWarning,_} = emysql:execute(PoolId,list_to_binary(SQL)),
	UnRepairedWarning.
	
%% 获取指定设备的所有告警  
-spec get_device_warning(string(),string()) -> list().
get_device_warning( DomainGuid,DevGuid ) ->
	SQL1 = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,RepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	
	SQL2 = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid++"';",
	lager:info("The SQL is : ~p~n",[SQL2]),
	{result_packet,_,_,UnRepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL2)),
	
	UnRepairedWarning++RepairedWarning.
	
%% 获取指定设备的所有告警  
-spec get_device_warning(string(),string(),string()) -> list().
get_device_warning( DomainGuid,DevGuid,Level ) ->
	SQL1 = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid++"' && level='"++Level++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,RepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	
	SQL2 = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid++"' && level='"++Level++"';",
	lager:info("The SQL is : ~p~n",[SQL2]),
	{result_packet,_,_,UnRepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL2)),
	
	UnRepairedWarning++RepairedWarning.
	
%% 获取指定设备的所有告警  
-spec get_device_warning(string(),string(),string(),string()) -> list().
get_device_warning( DomainGuid,DevGuid,StartTime,StopTime ) ->
	SQL1 = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid
	       ++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,RepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	
	SQL2 = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid
	       ++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	lager:info("The SQL is : ~p~n",[SQL2]),
	{result_packet,_,_,UnRepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL2)),
	
	UnRepairedWarning++RepairedWarning.
	
%% 获取指定设备的所有告警  
-spec get_device_warning(string(),string(),string(),string(),string()) -> list().
get_device_warning( DomainGuid,DevGuid,Level,StartTime,StopTime ) ->
	SQL1 = "SELECT * FROM warning_repaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid
	       ++"' && level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	lager:info("The SQL is : ~p~n",[SQL1]),
	{result_packet,_,_,RepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
	
	SQL2 = "SELECT * FROM warning_unrepaired WHERE domain_id='"++DomainGuid++"' && device_id='"++DevGuid
	       ++"' && level='"++Level++"' && start_time BETWEEN '"++StartTime++"' AND '"++StopTime++"';",
	lager:info("The SQL is : ~p~n",[SQL2]),
	{result_packet,_,_,UnRepairedWarning,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL2)),
	
	UnRepairedWarning++RepairedWarning.
	
%% 获取当前登陆用户订阅的告警码
-spec get_sub_warning_code(string(),string()) -> list().
get_sub_warning_code( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM sub_warning_code WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	CodeList = [Code || [_,_,_,Code] <- Rows],
	CodeList.
	
%% 获取当前登陆用户设置的告警通知邮件
-spec get_warning_notify_email(string(),string()) -> list().
get_warning_notify_email( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM warning_notify_email WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	EmailList = [Email || [_,_,_,Email] <- Rows],
	EmailList.
	
%% 获取当前登陆用户设置的告警通知电话号码
-spec get_warning_notify_phone(string(),string()) -> list().
get_warning_notify_phone( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM warning_notify_phone WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	PhoneList = [Phone || [_,_,_,Phone] <- Rows],
	PhoneList.
	
%% 获取当前登陆用户设置的告警过滤终端设备guid
-spec get_warning_terminal_guid(string(),string()) -> list().
get_warning_terminal_guid( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM warning_terminal_guid WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	GuidList = [Guid || [_,_,_,Guid] <- Rows],
	GuidList.
	
%% 获取当前登陆用户设置的告警过滤终端设备名称
-spec get_warning_terminal_name(string(),string()) -> list().
get_warning_terminal_name( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM warning_terminal_name WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	NameList = [Name || [_,_,_,Name] <- Rows],
	NameList.
	
%% 获取当前登陆用户设置的告警过滤服务器设备guid
-spec get_warning_server_guid(string(),string()) -> list().
get_warning_server_guid( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM warning_server_guid WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	GuidList = [Guid || [_,_,_,Guid] <- Rows],
	GuidList.
	
%% 获取当前登陆用户设置的告警过滤服务器设备名称
-spec get_warning_server_name(string(),string()) -> list().
get_warning_server_name( DomainGuid,UserID ) ->
	SQL = "SELECT * FROM warning_server_name WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Rows,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	%%lager:debug("The Result is : ~p~n",[Rows]),
	NameList = [Name || [_,_,_,Name] <- Rows],
	NameList.
	
%% 启用/停用告警过滤
-spec enable_warning_filter(string(),string(),string()) -> {ok,success}|{error,string()}.
enable_warning_filter( DomainGuid,UserID,Enable ) ->
	SQL = "SELECT * FROM warning_set WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO warning_set VALUES(NULL,'"++DomainGuid++"','"++UserID++"','"++Enable++"',NULL,NULL,NULL);",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_  ->
			SQL1 = "UPDATE warning_set SET enable_filter='"++Enable
			       ++"' WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 打开/关闭告警提示音
-spec enable_warning_music(string(),string(),string()) -> {ok,success}|{error,string()}.
enable_warning_music( DomainGuid,UserID,Enable ) ->
	SQL = "SELECT * FROM warning_set WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO warning_set VALUES(NULL,'"++DomainGuid++"','"++UserID++"',NULL,'"++Enable++"',NULL,NULL);",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_  ->
			SQL1 = "UPDATE warning_set SET enable_music='"++Enable
			       ++"' WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
	
%% 打开/关闭告警邮箱通知
-spec enable_warning_email(string(),string(),string()) -> {ok,success}|{error,string()}.
enable_warning_email( DomainGuid,UserID,Enable ) ->
	SQL = "SELECT * FROM warning_set WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO warning_set VALUES(NULL,'"++DomainGuid++"','"++UserID++"',NULL,NULL,'"++Enable++"',NULL);",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_  ->
			SQL1 = "UPDATE warning_set SET enable_email='"++Enable
			       ++"' WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 打开/关闭告警电话通知
-spec enable_warning_phone(string(),string(),string()) -> {ok,success}|{error,string()}.
enable_warning_phone( DomainGuid,UserID,Enable ) ->
	SQL = "SELECT * FROM warning_set WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	case Row of
		[] ->
			SQL1 = "INSERT INTO warning_set VALUES(NULL,'"++DomainGuid++"','"++UserID++"',NULL,NULL,NULL,'"++Enable++"');",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end;
		_  ->
			SQL1 = "UPDATE warning_set SET enable_phone='"++Enable
			       ++"' WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
			lager:info("The SQL is : ~p~n",[SQL1]),
	        Result = emysql:execute(nms_cache_pool,list_to_binary(SQL1)),
			lager:debug("The Result is : ~p~n",[Result]),
			case Result of
				{error_packet,_,_,_,Msg} ->
					{error,Msg};
				{ok_packet,_,_,_,_,_,_} ->
					{ok,success}
			end
	end.
	
%% 获取告警设置项
-spec get_warning_set(string(),string()) -> {string(),string(),string(),string()}.
get_warning_set(DomainGuid,UserID) ->
	SQL = "SELECT * FROM warning_set WHERE domain_id='"++DomainGuid++"' && user_id='"++UserID++"';",
	lager:info("The SQL is : ~p~n",[SQL]),
	{result_packet,_,_,Row,_} = emysql:execute(nms_cache_pool,list_to_binary(SQL)),
	case Row of
		[] ->
			{'0','1','1','1'};
		[[_,_,_,EnableFilter,EnableMusic,EnableEmail,EnablePhone]] ->
			{EnableFilter,EnableMusic,EnableEmail,EnablePhone}
	end.
	
%% 设置是否暂停所有告警(字符类型数据)
-spec set_stop_all_warning(pid(),string()) -> {ok,binary()}.
set_stop_all_warning(RedisClient,StopAll) ->
	eredis:q(RedisClient,["SET","stop_all_warning",StopAll]).
	
%% 获取是否暂停所有告警
-spec get_stop_all_warning(pid()) -> {ok,binary()}.
get_stop_all_warning(RedisClient) ->
	case eredis:q(RedisClient,["GET","stop_all_warning"]) of
		{ok,undefined} ->
			{ok,<<"0">>};
		{ok,Value} ->
			{ok,Value}
	end.
	
