-module(nms_manager_task_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-export([start_task_sup/5]).



%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor:start_link(?MODULE, []).

start_task_sup(Sup, TRef, MQArgs, RedisArgs, MySQLArgs) ->
    supervisor:start_child(Sup, [TRef, MQArgs, RedisArgs, MySQLArgs]).

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 0, 1}, 
          [{nms_task_sup, {nms_task_sup, start_link, []},
           temporary, infinity, supervisor, [nms_task_sup]}]}}.





