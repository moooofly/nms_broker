%% Copyright (c) 2014-2015, Moooofly <http://my.oschina.net/moooofly/blog>
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

-module(nms_config).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([set_manager_control/2]).
-export([get_manager_control/1]).
-export([set_task_control/2]).
-export([get_task_control/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TAB, ?MODULE).

-type monitors() :: [{{reference(), pid()}, any()}].
-record(state, {
	monitors = [] :: monitors()   
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec set_manager_control(nms_api:ref(), pid()) -> ok.
set_manager_control(Ref, Pid) ->
	true = gen_server:call(?MODULE, {set_manager_control, Ref, Pid}),
	ok.

-spec get_manager_control(nms_api:ref()) -> pid().
get_manager_control(Ref) ->
	P = ets:lookup_element(?TAB, {manager_control, Ref}, 2),
	io:format("ets:lookup_element(~p) => ~p~n", [Ref, P]),
	P.

-spec set_task_control(nms_api:ref(), pid()) -> ok.
set_task_control(Ref, Pid) ->
	true = gen_server:call(?MODULE, {set_task_control, Ref, Pid}),
	ok.

-spec get_task_control(nms_api:ref()) -> pid().
get_task_control(Ref) ->
	P = ets:lookup_element(?TAB, {task_control, Ref}, 2),
	io:format("ets:lookup_element(~p) => ~p~n", [Ref, P]),
	P.


init([]) ->
	Monitors1 = [{{erlang:monitor(process, Pid), Pid}, Ref} ||
		[Ref, Pid] <- ets:match(?TAB, {{manager_control, '$1'}, '$2'})],
	Monitors2 = [{{erlang:monitor(process, Pid), Pid}, Ref} ||
		[Ref, Pid] <- ets:match(?TAB, {{task_control, '$1'}, '$2'})],
	Monitors = Monitors1 ++ Monitors2,
	{ok, #state{monitors=Monitors}}.

handle_call({set_manager_control, Ref, Pid}, _, State=#state{monitors=Monitors}) ->
	case ets:insert_new(?TAB, {{manager_control, Ref}, Pid}) of
		true ->
			MonitorRef = erlang:monitor(process, Pid),
			{reply, true,
				State#state{monitors=[{{MonitorRef, Pid}, Ref}|Monitors]}};
		false ->
			{reply, false, State}
	end;
handle_call({set_task_control, Ref, Pid}, _, State=#state{monitors=Monitors}) ->
	case ets:insert_new(?TAB, {{task_control, Ref}, Pid}) of
		true ->
			MonitorRef = erlang:monitor(process, Pid),
			{reply, true,
				State#state{monitors=[{{MonitorRef, Pid}, Ref}|Monitors]}};
		false ->
			{reply, false, State}
	end;

handle_call(_Request, _From, State) ->
	{reply, ignore, State}.

handle_cast(_Request, State) ->
	{noreply, State}.


handle_info({'DOWN', MonitorRef, process, Pid, _}, State=#state{monitors=Monitors}) ->
    io:format("[nms_config] handle_info/2 recv DOWN message from ~p~n", [Pid]),

	{_, Ref} = lists:keyfind({MonitorRef, Pid}, 1, Monitors),
	ets:delete(?TAB, {manager_control, Ref}),
	ets:delete(?TAB, {task_control, Ref}),

	Monitors2 = lists:keydelete({MonitorRef, Pid}, 1, Monitors),
	{noreply, State#state{monitors=Monitors2}};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
