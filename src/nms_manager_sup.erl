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

-module(nms_manager_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(MRef) ->
    {ok, MSup} = supervisor:start_link(?MODULE, []),
    lager:info("[ManagerSup] ManagerSup Pid = ~p~n", [MSup]),
    {ok, MTSup}    = supervisor:start_child(
                         MSup, {nms_manager_task_sup,
                               {nms_manager_task_sup, start_link, []},
                               transient, infinity, supervisor,
                               [nms_manager_task_sup]}),
    lager:info("[ManagerSup] ManagerTaskSup Pid = ~p~n", [MTSup]),

    {ok, MControl} = supervisor:start_child(
                         MSup, {nms_manager_control,
                               {nms_manager_control, start_link, [MTSup, MRef]},
                               transient, brutal_kill, worker,
                               [nms_manager_control]}),
    lager:info("[ManagerSup] ManagerControl Pid = ~p~n", [MControl]),
    {ok, MSup, MControl}.

%%---------------------------------------------------------------------------
%% supervisor callback
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
