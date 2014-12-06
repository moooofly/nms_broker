-module(nms_api).

-include("nms_broker.hrl").

-export([start_default/1]).
-export([start_custom/2, start_custom/5]).
-export([start_task_group_only/2]).
-export([start_task/1]).

-export([consume/2]).
-export([to_binary/1]).
-export([gen_object_name/0]).


-type ref() :: any().
-export_type([ref/0]).


-spec start_default(ref()) -> {ok, pid()} | {error, badarg}.
%% 
%% MRef -> 用于标识以 nms_manager_sup 作为根的监督树结构
%% 
start_default(MRef) ->
    ensure_started(),
    {ok, _MSup, ManControl} = nms_broker_sup:start_manager_sup(child_spec(MRef)),
    {ok, TRef, TaskControl} = nms_manager_control:start_task(ManControl),
    {ok, {manager_control, MRef, ManControl}, {task_control, TRef, TaskControl}}.

%% 通过定制参数控制 task 的启动，其中 #args{mq_args,redis_args,mysql_args} 包含
%% 的参数说明如下
%% 
%% mq_args -> rabbitmq client 启动参数，默认值为 none ，表示不启动相应 task；
%%            若为 [] ，则表示使用默认值进行启动；若要定制启动参数，则通过
%%            proplists 的方式进行设置；
%% redis_args -> redis client 启动参数，说明同上；
%% mysql_args -> rabbitmq client 启动参数，说明同上。
%% 
start_custom(MRef, #args{} = Args) ->
    ensure_started(),
    {ok, _MSup, ManControl} = nms_broker_sup:start_manager_sup(child_spec(MRef)),
    {ok, TRef, TaskControl} = nms_manager_control:start_task(ManControl, Args),
    {ok, {manager_control, MRef, ManControl}, {task_control, TRef, TaskControl}}.

start_custom(MRef, TRef, RabbitConfig, RedisConfig, MySQLConfig) ->
    ensure_started(),
    Args = #args{task_ref=TRef, mq_args=RabbitConfig, redis_args=RedisConfig, mysql_args=MySQLConfig},
    {ok, _MSup, ManControl} = nms_broker_sup:start_manager_sup(child_spec(MRef)),
    {ok, TRef, TaskControl} = nms_manager_control:start_task(ManControl, Args),
    {ok, {manager_control, MRef, ManControl}, {task_control, TRef, TaskControl}}.

%% 在由 MRef 确定的 nms_manager_task_sup 进程下创建一组 task 工作进程
%% 前置条件：要求已成功建立 nms_manager_sup + nms_manager_task_sup + nms_manager_control
%%           进程结构
start_task_group_only(MRef, #args{} = Args) ->
    ManControl = nms_config:get_manager_control(MRef),
    {ok, TRef, TaskControl} = nms_manager_control:start_task(ManControl, Args),
    {ok, {manager_control, MRef, ManControl}, {task_control, TRef, TaskControl}}.



%% consume test
%%
%% 通过该函数可以直接向 nms_task_control 进程发送待执行命令
%% 由于底层 rabbitmq 要求 queue 的名字必须为二进制字符串，故此处做必要转换
%% 
consume(TaskCon, QueueN) when is_pid(TaskCon) ->
    nms_task_control:do_consume(TaskCon, to_binary(QueueN));
consume(TRef, QueueN) ->
    TaskCon = nms_config:get_task_control(TRef),
    nms_task_control:do_consume(TaskCon, to_binary(QueueN)).



%% 通过指定 nms_manager_control 进程 pid ，间接对 nms_manager_task_sup 进程进行操作
%% ManControl -> nms_manager_control 进程的 pid
start_task(ManControl) when is_pid(ManControl) ->
    nms_manager_control:start_task(ManControl);
start_task(Ref) ->
    ManControl = nms_config:get_manager_control(Ref),
    nms_manager_control:start_task(ManControl).

ensure_started() ->
    ok.


-spec child_spec(ref()) -> supervisor:child_spec().
child_spec(Ref) ->
    {{nms_manager_sup, Ref}, 
     {nms_manager_sup, start_link, [Ref]}, 
      temporary, infinity, supervisor, [nms_manager_sup]}.


to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_binary(X)  -> X;
to_binary(X) when is_integer(X) -> list_to_binary(integer_to_list(X));
to_binary(X) when is_float(X)   -> throw({cannot_store_floats, X});
to_binary(X)                    -> term_to_binary(X).


%% @spec (Nibble::integer()) -> char()
%% @doc Returns the character code corresponding to Nibble.
%%
%% Nibble must be >=0 and =<16.
hex_digit(0) -> $0;
hex_digit(1) -> $1;
hex_digit(2) -> $2;
hex_digit(3) -> $3;
hex_digit(4) -> $4;
hex_digit(5) -> $5;
hex_digit(6) -> $6;
hex_digit(7) -> $7;
hex_digit(8) -> $8;
hex_digit(9) -> $9;
hex_digit(10) -> $A;
hex_digit(11) -> $B;
hex_digit(12) -> $C;
hex_digit(13) -> $D;
hex_digit(14) -> $E;
hex_digit(15) -> $F.

binary_to_hex(<<>>) ->
    [];
binary_to_hex(<<B, Rest/binary>>) ->
    [hex_digit((B bsr 4) band 15), hex_digit(B band 15) | binary_to_hex(Rest)].

%% @spec () -> string()
%% @doc Generates a unique name that can be used for otherwise unnamed JSON-RPC services.
gen_object_name() ->
    Hash = erlang:md5(term_to_binary({node(), erlang:now()})),
    binary_to_hex(Hash).
