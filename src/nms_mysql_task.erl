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

-module(nms_mysql_task).
-behaviour(gen_server).

-export([init/1,
         terminate/2,
         code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-export([start_link/2]).

-record(pool, {
            pool_id = undefined :: undefined | nms_api:ref(), 
            options = [] :: list()
        }).

-record(state, {
            tref      = undefined :: undefined | nms_api:ref(),
            pool_info = #pool{},
            status    = off :: off | on
    }).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

-spec start_link(PoolId, Args) -> {ok, Pid} | {error, Reason} when
    PoolId :: nms_api:ref(),
    Args   :: list(),
    Pid    :: pid(),
    Reason :: term().
start_link(PoolId, Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [PoolId, Args], []);
start_link(_, _) ->
    lager:error("[MySQLTask] Args must be list! Error!"),
    {error, args_not_list}.

%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

-spec ensure_emysql_started() -> ok.
ensure_emysql_started() ->
    case application_controller:get_master(emysql) of
        undefined ->
            crypto:start(),
            case emysql:start() of
                ok                                      -> ok;
                {error, {already_started, emysql}}      -> ok;
                {error, _} = E                          -> throw(E)
            end;
        _ ->
            ok
    end.

initialize_pool_status(Pool, Status) ->
    PoolId = Pool#pool.pool_id,
    case Status of
        off ->
            case emysql:add_pool(Pool#pool.pool_id, Pool#pool.options) of 
                ok ->
                    lager:notice("[MySQLTask] PoolId(~p) create Success!~n", [PoolId]),
                    ok;        
                {error, pool_already_exists} ->
                    lager:notice("[MySQLTask] PoolId(~p) already Exists~n", [PoolId]),
                    ok;
                {error, Error} ->
                    lager:error("[MySQLTask] get {error, ~p}~n", [Error]),
                    not_ok
            end;
        on ->
            ok
    end.


%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

init([PoolId, Args]) ->
    process_flag(trap_exit, true),

%%    Size      = proplists:get_value(size, Args, 1),
%%    User      = proplists:get_value(user, Args, "root"),
%%    Password  = proplists:get_value(password, Args, "root"),
%%    Host      = proplists:get_value(host, Args, "172.16.81.101"),
%%    Port      = proplists:get_value(port, Args, 3306),
%%    Database  = proplists:get_value(database, Args, "nms_db"),
%%    Encoding  = proplists:get_value(encoding, Args, utf8),
%%    StartCmds = proplists:get_value(start_cmds, Args, []),
%%    ConnectTimeout = proplists:get_value(connect_timeout, Args, infinity),
    Pool = #pool{pool_id=PoolId, options=Args},

%%    lager:notice("~s~n", [string:chars($-,72)]),
%%    lager:notice("~n"
%%                "    PoolId=~p~n"
%%                "    Size=~p~n"
%%                "    User=~p~n"
%%                "    Password=~p~n"
%%                "    Host=~p~n"
%%                "    Port=~p~n"
%%                "    Database=~p~n"
%%                "    Encoding=~p~n"
%%                "    StartCmds=~p~n"
%%                "    ConnectTimeout=~p~n", 
%%        [PoolId,Size,User,Password,Host,Port,Database,Encoding,StartCmds,ConnectTimeout]), 
%%    lager:notice("~s~n", [string:chars($-,72)]),

    case catch ensure_emysql_started() of
        ok ->
            nms_config:set_mysql_task(PoolId, self()),
            {ok, #state{tref=PoolId, pool_info=Pool, status=off}};
        {error, Error} ->
            {stop, {emysql_startapp_error, Error}}
    end.    


handle_call( {get_terminal_mem_limit}, _From, #state{pool_info=Pool,status=Status}=State ) ->
    lager:critical("[MySQLTask] PoolId(~p) <== Recv 'get_terminal_mem_limit'~n", [Pool#pool.pool_id]),

    case initialize_pool_status(Pool, Status) of
        ok ->
            % Value = system_set_handler:get_terminal_mem_limit_mysql(Pool#pool.pool_id),
            {reply, {ok, fakeValue}, State#state{status=on}};
        _ ->
            lager:warning("[MySQLTask] get_terminal_mem_limit failed!~n", []),
            {reply, pool_init_failed, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', Pid, Info}, State) ->
    lager:warning("[MySQLTask] recv {'EXIT', ~p, ~p}~n", [Pid, Info]),
    {noreply, State};

handle_info(Info, State) ->
    lager:notice("[MySQLTask] handle_info => Info(~p)~n", [Info]),
    {noreply, State}.

terminate(Reason, #state{tref=TRef}=_State) ->
    lager:warning("[MySQLTask] terminate => Reason(~p)~n", [Reason]),
    TaskConPid = nms_config:get_task_control(TRef),
    exit(TaskConPid, {restart, from_mysql_task}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.