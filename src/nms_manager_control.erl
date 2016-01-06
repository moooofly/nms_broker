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

-module(nms_manager_control).

-include("nms_broker.hrl").

-behaviour(gen_server).

-export([start_link/2]).
-export([start_task/1, start_task/2]).

-export([init/1, 
         terminate/2, 
         code_change/3, 
         handle_call/3, 
         handle_cast/2,
         handle_info/2]
       ).


-record(state, {
                    mt_sup,     %% nms_manager_task_sup 进程 pid
                    mref        %% 标识以 nms_manager_sup 为根的监督结构
	           }).


%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

%% 用于在 nms_manager_task_sup 进行下动态添加 nms_task_sup 进程树结构，即 task 组
%% 
%% MTSup -> nms_manager_task_sup 进程 pid
%% MRef -> 用于标识以 nms_manager_sup 为根的监督结构
%% 
start_link( MTSup, MRef ) ->
    gen_server:start_link(?MODULE, [MTSup, MRef], []).


%% MConPid -> nms_manager_control 进程的 pid
start_task( MConPid ) ->
    gen_server:call(MConPid, start_task, infinity).

start_task( MConPid, #args{} = Args ) ->
    gen_server:call(MConPid, {start_task, Args}, infinity).


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init( [MTSup, MRef] ) ->
    ok = nms_config:set_manager_control(MRef, self()),
    io:format("===>  nms_config:set_manager_control = [~p,~p]~n", [MRef, self()]),
    {ok, #state{mt_sup=MTSup, mref=MRef}}.


%% 以 default 参数启动 task
handle_call( start_task, _From, #state{mt_sup=MTSup} = State ) ->
    TRef = nms_api:gen_object_name(),
    io:format("generate Random TaskName => ~p~n", [TRef]),
    {ok,_,TaskControl} = 
        nms_manager_task_sup:start_task_sup(MTSup, TRef, [], [], []),
    {reply, {ok, TRef, TaskControl}, State};

%% 以自定义参数启动 task
handle_call( {start_task, 
        #args{task_ref=TRef,mq_args=MQArgs,redis_args=RDArgs,mysql_args=MSArgs}}, 
        _From, #state{mt_sup=MTSup} = State ) ->

    TRef1 = case TRef of
        undefined ->
            nms_api:gen_object_name();
        _ ->
            TRef
    end,
    {ok,_,TaskControl} = 
        nms_manager_task_sup:start_task_sup(MTSup, TRef1, MQArgs, RDArgs, MSArgs),
    {reply, {ok, TRef1, TaskControl}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
