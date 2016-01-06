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

