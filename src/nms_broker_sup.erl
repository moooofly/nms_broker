-module(nms_broker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_manager_sup/1]).

%% Supervisor callbacks
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Ref -> 用于标识以 nms_manager_sup 作为根的监督树结构
start_manager_sup(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([]) ->
    nms_config = ets:new(nms_config, [ordered_set, public, named_table]),
	Procs = [
		{nms_config, {nms_config, start_link, []}, 
		      permanent, 5000, worker, [nms_config]}
	],
	{ok, {{one_for_one, 10, 10}, Procs}}.

