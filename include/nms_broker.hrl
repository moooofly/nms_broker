-ifndef(NMS_BROKER_HRL).
-define(NMS_BROKER_HRL, true).

-record(args, {
                  task_ref   = undefined, %% 标识 task 组的标识
                  mq_args    = none,  %% proplists ，若为 [] 则使用默认值，none 表示不启动
                  redis_args = none,  %% proplists ，若为 [] 则使用默认值，none 表示不启动
                  mysql_args = none   %% proplists ，若为 [] 则使用默认值，none 表示不启动
               }).

-endif.