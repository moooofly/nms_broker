-module(nms_broker_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([start/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    nms_broker_sup:start_link().

stop(_State) ->
    ok.

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start() ->
    application:start(nms_broker).
