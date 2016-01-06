%% Copyright (c) 2014-2016, Moooofly <http://my.oschina.net/moooofly/blog>
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