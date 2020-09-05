# Impalawork

### Note About Impala Learning

### A HomeWork For Impala Learning

###  业务背景

现有收集到用户的页面点击行为日志数据，数据格式如下：

用户id, 点击时间

```txt
user_id click_time
A,2020-05-15 01:30:00
A,2020-05-15 01:35:00
A,2020-05-15 02:00:00
A,2020-05-15 03:00:10
A,2020-05-15 03:05:00
B,2020-05-15 02:03:00
B,2020-05-15 02:29:40
B,2020-05-15 04:00:00
```

业务：

会话概念：用户的一次会话含义是指用户进入系统开始到用户离开算作一次会话，离开或者重新开始一次会话的概念是指用户的两次行为事件差值大于30分钟，

比如以A用户为例：

第一次会话

```txt
A,2020-05-15 01:30:00
A,2020-05-15 01:35:00
A,2020-05-15 02:00:00
```

第二次会话

```txt
A,2020-05-15 03:00:10
A,2020-05-15 03:05:00
```

判断条件是只要两次时间差值大于30分钟就属于两次会话。

## 需求

对用户的日志数据打上会话内序号，如下

```txt
A,2020-05-15 01:30:00,1
A,2020-05-15 01:35:00,2
A,2020-05-15 02:00:00,3
A,2020-05-15 03:00:10,1
A,2020-05-15 03:05:00,2
B,2020-05-15 02:03:00,1
B,2020-05-15 02:29:40,2
B,2020-05-15 04:00:00,1
```

### 实现及思路

在Hive中完成数据加载

```sql
--创建表
drop table if exists user_clicklog;
create table user_clicklog ( 
        user_id string, 
        click_time string
        )
row format delimited fields terminated by ",";

--加载数据
load data local inpath '/root/impala_data/clicklog.dat' into table user_clicklog; 
```

使用Impala sql完成指标统计...



使用Impala sql完成指标统计...

作业思路：

1 根据用户id分组按照时间进行排序， 使用lag函数将上下数据关联

2 使用case when根据条件进行判断，构造切分列

3 对切分列进行累计求和，对会话进行切分

4 最后使用row_number函数进行排序

```sql
with q1 as (
select user_id, click_time, 
case when unix_timestamp(lead(click_time) over(partition by user_id order by click_time)) - unix_timestamp(click_time) > 1800 then 1 else 0 end split_flag 
from user_clicklog
),
q2 as (
select user_id, click_time, 
sum(split_flag) over(partition by user_id order by click_time) as session_num 
from q1
)
select user_id, click_time, 
row_number() over(partition by user_id, session_num order by click_time) 
from q2
```


This a impala learning study note and homework
