-module(meeting_handler).

-export([
		 add_terminal_enter_meeting_info/5,       %% add
		 add_terminal_leave_meeting_info/6,       %% add

		 inc_terminal_enter_meeting_times/3,      %% add
		 get_terminal_enter_meeting_times/3,      %% add

		 inc_port_meeting_terminal_num/2,         %% add
		 dec_port_meeting_terminal_num/2,         %% add

		 inc_traditional_meeting_terminal_num/2,  %% add
		 dec_traditional_meeting_terminal_num/2,  %% add

		 add_traditional_meeting/8,     %% update
         del_traditional_meeting/2,     %% update

         add_port_meeting/9,            %% update
		 del_port_meeting/2,

		 add_p2p_meeting/11,            %% update
		 del_p2p_meeting/2,             %% update

		 add_meeting_terminal/3,
		 del_meeting_terminal/3,
		 add_meeting_terminals/3,
		 update_terminal_meeting_detail/18]).



%% 添加终端进会相关信息
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_enter_meeting_info(pid(),string(),string(),string(),string()) -> {ok,binary()}.
add_terminal_enter_meeting_info(RedisClient,TerminalMoid,ConfE164,TerEnterTimes,TerEnterTime) ->
	Key = format_key_terminal_enter_leave_meeting(TerminalMoid,ConfE164,TerEnterTimes),
	eredis:q(RedisClient,["HSET",Key,"enter_time",TerEnterTime]).

%% 添加终端退会相关信息
%% 返回值 : {ok,<<"OK">>}
-spec add_terminal_leave_meeting_info(pid(),string(),string(),string(),string(),string()) -> {ok,binary()}.
add_terminal_leave_meeting_info(RedisClient,TerminalMoid,ConfE164,TerEnterTimes,TerLeaveTime,TerLeaveReason) ->
	Key = format_key_terminal_enter_leave_meeting(TerminalMoid,ConfE164,TerEnterTimes),
	eredis:q(RedisClient,["HMSET",Key,"leave_time",TerLeaveTime,"leave_reason",TerLeaveReason]).

%% 增加会议终端数目
%% 返回值 : {ok,<<"Num">>}
-spec inc_terminal_enter_meeting_times(pid(),string(),string()) -> {ok,binary()}.
inc_terminal_enter_meeting_times(RedisClient,TerminalMoid,ConfE164) ->
	Key = format_key_terminal_enter_meeting_times(TerminalMoid,ConfE164),
	eredis:q(RedisClient,["INCRBY",Key,"1"]).

%% 获取会议终端数目
%% 返回值 : {ok,<<"Num">>}
-spec get_terminal_enter_meeting_times(pid(),string(),string()) -> {ok,binary()}.
get_terminal_enter_meeting_times(RedisClient,TerminalMoid,ConfE164) ->
	Key = format_key_terminal_enter_meeting_times(TerminalMoid,ConfE164),
	eredis:q(RedisClient,["INCRBY",Key,"0"]).

%% 增加端口会议终端数目
%% 返回值 : {ok,<<"Num">>}
-spec inc_port_meeting_terminal_num(pid(),string()) -> {ok,binary()}.
inc_port_meeting_terminal_num(RedisClient,ConfE164) ->
	Key = format_key_p_meeting_info(ConfE164),
	eredis:q(RedisClient,["HINCRBY",Key,"terminal","1"]).

%% 减少端口会议终端数目
%% 返回值 : {ok,<<"Num">>}
-spec dec_port_meeting_terminal_num(pid(),string()) -> {ok,binary()}.
dec_port_meeting_terminal_num(RedisClient,ConfE164) ->
	Key = format_key_p_meeting_info(ConfE164),
	eredis:q(RedisClient,["HINCRBY",Key,"terminal","-1"]).

%% 增加传统会议终端数目
%% 返回值 : {ok,<<"Num">>}
-spec inc_traditional_meeting_terminal_num(pid(),string()) -> {ok,binary()}.
inc_traditional_meeting_terminal_num(RedisClient,ConfE164) ->
	Key = format_key_t_meeting_info(ConfE164),
	eredis:q(RedisClient,["HINCRBY",Key,"terminal","1"]).

%% 减少传统会议终端数目
%% 返回值 : {ok,<<"Num">>}
-spec dec_traditional_meeting_terminal_num(pid(),string()) -> {ok,binary()}.
dec_traditional_meeting_terminal_num(RedisClient,ConfE164) ->
	Key = format_key_t_meeting_info(ConfE164),
	eredis:q(RedisClient,["HINCRBY",Key,"terminal","-1"]).

%% 添加一个传统会议(散列类型数据)
%% 添加会议信息的同时,把会议ID添加到会议所属域下
%% 返回值 : 
-spec add_traditional_meeting(pid(),string(),string(),string(),integer(),string(),string(),string()) -> {ok,success}.
add_traditional_meeting(RedisClient,DomainMoid,MeetingID,Name,BandWidth,Device,StartTime,StopTime ) ->

	KeyMeeting = format_key_t_meeting(DomainMoid),
	KeyInfo = format_key_t_meeting_info(MeetingID),
	
	UName = binary_to_list(unicode:characters_to_binary(Name)),
	UDevice = binary_to_list(unicode:characters_to_binary(Device)),

	KeyValuePairs = [
						KeyInfo,
						"e164",			MeetingID,
						"domain_moid",	DomainMoid,
						"name",			UName,
						"bandwidth",	BandWidth,
						"device",		UDevice,
						"start_time",	StartTime,
						"stop_time",	StopTime
					],

	eredis:q(RedisClient,["MULTI"]),
    eredis:q(RedisClient,["SADD",KeyMeeting,MeetingID]),			
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]),
	eredis:q(RedisClient,["EXEC"]).

%% 删除一个传统会议(散列类型数据)
%% 需要同时删除传统会议的信息,会议里面的终端,以及会议所属域下相应的会议ID
%% 返回值 : {error,<<"Key Error">>}|{ok,success}
-spec del_traditional_meeting(pid(),string()) -> {ok,binary()}|{ok,success}.
del_traditional_meeting(RedisClient,MeetingID) ->
	KeyInfo = format_key_t_meeting_info(MeetingID),

	case eredis:q(RedisClient,["HGET",KeyInfo,"domain_moid"]) of
		{ok,undefined} ->
			{error,<<"Key Error">>};
		{ok,DomainMoid} ->
			KeyMeeting = format_key_t_meeting(DomainMoid),
			KeyTerminal = format_key_meeting_terminal(MeetingID),

			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",KeyMeeting,MeetingID]),			
			eredis:q(RedisClient,["DEL",KeyInfo]),
			eredis:q(RedisClient,["DEL",KeyTerminal]),
			eredis:q(RedisClient,["EXEC"])
	end.
	
%% 添加一个端口会议(散列类型数据)
%% 添加会议信息的同时,把会议ID添加到会议所属域下
%% 返回值 : {ok,success}
-spec add_port_meeting(pid(),string(),string(),string(),integer(),integer(),integer(),string(),string()) -> {ok,success}.	
add_port_meeting(RedisClient,DomainMoid,MeetingID,Name,BandWidth,Terminal,Port,StartTime,StopTime) ->

	KeyMeeting = format_key_p_meeting(DomainMoid),
	KeyInfo = format_key_p_meeting_info(MeetingID),
	
	UName = binary_to_list(unicode:characters_to_binary(Name)),
	KeyValuePairs = [
						KeyInfo,
						"id",			MeetingID,
						"domain_moid",	DomainMoid,
						"name",			UName,
						"bandwidth",	BandWidth,
						"terminal",		Terminal,
						"port",			Port,
						"start_time",	StartTime,
						"stop_time",	StopTime
					],
			
	eredis:q(RedisClient,["MULTI"]),
    eredis:q(RedisClient,["SADD",KeyMeeting,MeetingID]),			
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]),
	eredis:q(RedisClient,["EXEC"]).


%% 删除一个端口会议(散列类型数据)
%% 需要同时删除传统会议的信息,会议里面的终端,以及会议所属域下相应的会议ID
%% 返回值 : {error,<<"Key Error">>}|{ok,success}
-spec del_port_meeting(pid(),string()) -> {ok,binary()}|{ok,success}.	
del_port_meeting(RedisClient,MeetingID) ->
	KeyInfo = format_key_p_meeting_info(MeetingID),

	case eredis:q(RedisClient,["HGET",KeyInfo,"domain_moid"]) of
		{ok,undefined} ->
			{error,<<"Key Error">>};
		{ok,DomainMoid} ->
			KeyMeeting = format_key_p_meeting(DomainMoid),
			KeyTerminal = format_key_meeting_terminal(MeetingID),

			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",KeyMeeting,MeetingID]),			
			eredis:q(RedisClient,["DEL",KeyInfo]),
			eredis:q(RedisClient,["DEL",KeyTerminal]),
			eredis:q(RedisClient,["EXEC"])
	end.
	
%% 添加一个点对点会议(散列类型数据)
%% 添加会议信息的同时,把会议ID添加到会议所属域下
%% 返回值 : 
-spec add_p2p_meeting(pid(),string(),string(),string(),string(),string(),string(),string(),string(),integer(),string()) -> {ok,success}.
add_p2p_meeting(RedisClient,CallerDomainMoid,CallerE164,CallerName,CallerType,
		CalleeDomainMoid,CalleeE164,CalleeName,CalleeType,Bandwidth,StartTime) ->

	KeyCallerMeeting = format_key_p2p_meeting(CallerDomainMoid),
	KeyCalleeMeeting = format_key_p2p_meeting(CalleeDomainMoid),
	KeyInfo = format_key_p2p_meeting_info(CallerE164),
	
	UCallerName = binary_to_list(unicode:characters_to_binary(CallerName)),
	UCalleeName = binary_to_list(unicode:characters_to_binary(CalleeName)),
	KeyValuePairs = [
						KeyInfo,
						"caller_domain_moid",	CallerDomainMoid,
						"caller_e164",			CallerE164,
						"caller_name",			UCallerName,
						"caller_type",			CallerType,
						"callee_domain_moid",	CalleeDomainMoid,
						"callee_e164",			CalleeE164,
						"callee_name",			UCalleeName,
						"callee_type",			CalleeType,
						"bandwidth",			Bandwidth,
						"start_time",			StartTime
					],
			
	eredis:q(RedisClient,["MULTI"]),
    eredis:q(RedisClient,["SADD",KeyCallerMeeting,CallerE164]),	
    eredis:q(RedisClient,["SADD",KeyCalleeMeeting,CallerE164]),	
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]),
	eredis:q(RedisClient,["EXEC"]).
	
%% 删除一个点对点会议(散列类型数据)
%% 需要同时删除传统会议的信息,以及会议所属域下相应的会议ID
%% 返回值 : {error,<<"Key Error">>}|{ok,success}
-spec del_p2p_meeting(pid(),string()) -> {ok,binary()}|{ok,success}.	
del_p2p_meeting(RedisClient,CallerE164) ->
	KeyInfo = format_key_p2p_meeting_info(CallerE164),

	CallerDomainResult = eredis:q(RedisClient,["HGET",KeyInfo,"caller_domain_moid"]),
	CalleeDomainResult = eredis:q(RedisClient,["HGET",KeyInfo,"callee_domain_moid"]),
	if 
		CallerDomainResult =/= {ok,undefined} andalso CalleeDomainResult =/= {ok,undefined} ->
		    
		    {ok,CallerDomainMoid} = CallerDomainResult,
		    {ok,CalleeDomainMoid} = CalleeDomainResult,

			KeyCallerMeeting = format_key_p2p_meeting(CallerDomainMoid),
			KeyCalleeMeeting = format_key_p2p_meeting(CalleeDomainMoid),
			
			eredis:q(RedisClient,["MULTI"]),
			eredis:q(RedisClient,["SREM",KeyCallerMeeting,CallerE164]),
            eredis:q(RedisClient,["SREM",KeyCalleeMeeting,CallerE164]),			
			eredis:q(RedisClient,["DEL",KeyInfo]),
			eredis:q(RedisClient,["EXEC"]);
		true ->
			{error,<<"Key Error or Key not Exist">>}
	end.
	
%% 添加一个会议终端(集合类型数据)
%% 返回值 : {ok,<<"1">>}
-spec add_meeting_terminal(pid(),string(),string()) -> {ok,binary()}.
add_meeting_terminal(RedisClient,MeetingID,TerminalE164) ->
	Key = format_key_meeting_terminal(MeetingID),
	eredis:q(RedisClient,["SADD",Key,TerminalE164]).
	
%% 删除一个会议终端(集合类型数据)
%% 返回值 : {ok,<<"1">>}
-spec del_meeting_terminal(pid(),string(),string()) -> {ok,binary()}.
del_meeting_terminal(RedisClient,MeetingID,TerminalE164) ->
	Key = format_key_meeting_terminal(MeetingID),
	eredis:q(RedisClient,["SREM",Key,TerminalE164]).
	
%% 添加多个会议终端(集合类型数据)
%% 返回值 : {ok,success}
-spec add_meeting_terminals(pid(),string(),list()) -> {ok,success}.
add_meeting_terminals(RedisClient,MeetingID,TerminalList) ->
	Key = format_key_meeting_terminal(MeetingID),
	Fun = fun( TerminalE164, Acc ) ->
			[["SADD",Key,TerminalE164]|Acc]
		  end,
		  
	Pipeline = lists:foldl( Fun,[],TerminalList ),
	eredis:qp(RedisClient,Pipeline),
	
	{ok,success}.

%% 更新终端设备的会议详细信息
-spec update_terminal_meeting_detail(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) -> {}.	
update_terminal_meeting_detail(RedisClient, DevMoid, MeetingID,AudioLostRate,AudioTotalLose,VideoLostRate,VideoTotalLose,
			                   Latency,NatIP,NatIPOperator,RetisterAddress,RegisterOperator,AudioUpRate,
			                   AudioDownRate,VideoUpRate,VideoDownRate,VideoResourceExist,VideoFrameRate) ->
	Key = format_key_meeting_detail( DevMoid ),
	KeyValuePairs = [Key,"meeting_id",MeetingID,"audio_lost_rate",AudioLostRate,"audio_total_lose",AudioTotalLose,
					"video_lost_rate",VideoLostRate,"video_total_lose",VideoTotalLose,"latency",Latency,
					"nat_ip",NatIP,"nat_ip_operator",NatIPOperator,"register_address",RetisterAddress,
					"register_ operator",RegisterOperator,"audio_up_rate",AudioUpRate,"audio_down_rate",
					AudioDownRate,"video_up_rate",VideoUpRate,"video_down_rate",VideoDownRate,"video_resource_exist",
					VideoResourceExist,"video_frame_rate",VideoFrameRate],
	eredis:q(RedisClient,["HMSET"|KeyValuePairs]).


format_key_terminal_enter_leave_meeting(TerminalMoid,ConfE164,TerEnterTimes)
	when is_list(TerminalMoid) andalso is_list(ConfE164) andalso is_list(TerEnterTimes)      ->
	"terminal:" ++ TerminalMoid ++ ":conf:" ++ ConfE164 ++ ":enter_leave_info:" ++ TerEnterTimes;
format_key_terminal_enter_leave_meeting(TerminalMoid,ConfE164,TerEnterTimes)
	when is_binary(TerminalMoid) andalso is_binary(ConfE164) andalso is_binary(TerEnterTimes) ->
	"terminal:" ++ binary_to_list(TerminalMoid) ++ 
	":conf:" ++ binary_to_list(ConfE164) ++ 
	":enter_leave_info:" ++ binary_to_list(TerEnterTimes).

format_key_terminal_enter_meeting_times(TerminalMoid,ConfE164) when is_list(TerminalMoid) andalso is_list(ConfE164)     ->
	"terminal:" ++ TerminalMoid ++ ":conf:" ++ ConfE164 ++ ":enter_times";
format_key_terminal_enter_meeting_times(TerminalMoid,ConfE164) when is_binary(TerminalMoid) andalso is_binary(ConfE164) ->
	"terminal:" ++ binary_to_list(TerminalMoid) ++ ":conf:" ++ binary_to_list(ConfE164) ++ ":enter_times".

format_key_t_meeting(DomainMoid) when is_list(DomainMoid)->"domain:" ++ DomainMoid ++ ":t_meeting";
format_key_t_meeting(DomainMoid) when is_binary(DomainMoid)->"domain:" ++ binary_to_list(DomainMoid) ++ ":t_meeting".

format_key_t_meeting_info(MeetingID) when is_list(MeetingID)->"t_meeting:" ++ MeetingID ++ ":info";
format_key_t_meeting_info(MeetingID) when is_binary(MeetingID)->"t_meeting:" ++ binary_to_list(MeetingID) ++ ":info".

format_key_p_meeting(DomainMoid) when is_list(DomainMoid)->"domain:" ++ DomainMoid ++ ":p_meeting";
format_key_p_meeting(DomainMoid) when is_binary(DomainMoid)->"domain:" ++ binary_to_list(DomainMoid) ++ ":p_meeting".

format_key_p_meeting_info(MeetingID) when is_list(MeetingID)->"p_meeting:" ++ MeetingID ++ ":info";
format_key_p_meeting_info(MeetingID) when is_binary(MeetingID)->"p_meeting:" ++ binary_to_list(MeetingID) ++ ":info".

format_key_p2p_meeting(DomainMoid) when is_list(DomainMoid)->"domain:" ++ DomainMoid ++ ":p2p_meeting";
format_key_p2p_meeting(DomainMoid) when is_binary(DomainMoid)->"domain:" ++ binary_to_list(DomainMoid) ++ ":p2p_meeting".

format_key_p2p_meeting_info(MeetingID) when is_list(MeetingID)->"p2p_meeting:" ++ MeetingID ++ ":info";
format_key_p2p_meeting_info(MeetingID) when is_binary(MeetingID)->"p2p_meeting:" ++ binary_to_list(MeetingID) ++ ":info".

format_key_meeting_terminal(MeetingID) when is_list(MeetingID)->"meeting:" ++ MeetingID ++ ":terminal";
format_key_meeting_terminal(MeetingID) when is_binary(MeetingID)->"meeting:" ++ binary_to_list(MeetingID) ++ ":terminal".

format_key_meeting_detail(DevMoid) when is_list(DevMoid)->"terminal:" ++ DevMoid ++ ":meeting_detail";
format_key_meeting_detail(DevMoid) when is_binary(DevMoid)->"terminal:" ++ binary_to_list(DevMoid) ++ ":meeting_detail".