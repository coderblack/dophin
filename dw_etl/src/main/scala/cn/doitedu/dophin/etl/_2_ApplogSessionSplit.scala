package cn.doitedu.dophin.etl

import org.apache.spark.sql.SparkSession

/**
 * app端日志的session分割处理
 * 先建好一个临时表：用于存放 会话切割 之后的结果
 *
create table tmp.mall_applog_session_split(
    account              string
    ,app_id              string
    ,app_version         string
    ,carrier             string
    ,device_id           string
    ,device_type         string
    ,event_id            string
    ,ip                  string
    ,latitude            double
    ,longitude           double
    ,net_type            string
    ,os_name             string
    ,os_version          string
    ,properties          map<string,string>
    ,release_channel     string
    ,resolution          string
    ,session_id          string
    ,ts                  bigint
    ,new_session_id      string
)
partitioned by (dt string)
stored as orc
tblproperties('orc.compress'='snappy')
;
 */
object _2_ApplogSessionSplit {

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println(
        """
          |usage:必须传入1个参数
          |  参数1:待处理的日期，如 2022-02-16
          |""".stripMargin)
      sys.exit(1)
    }

    val dt:String = args(0)

    val spark = SparkSession.builder()
      //.config("spark.sql.shuffle.partitions",2)
      .appName("app端日志数据，清洗过滤计算")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      s"""
        |insert into table tmp.mall_applog_session_split partition(dt='${dt}')
        |select
        |       account
        |       ,app_id
        |       ,app_version
        |       ,carrier
        |       ,device_id
        |       ,device_type
        |       ,event_id
        |       ,ip
        |       ,latitude
        |       ,longitude
        |       ,net_type
        |       ,os_name
        |       ,os_version
        |       ,properties
        |       ,release_channel
        |       ,resolution
        |       ,session_id
        |       ,ts
        |       ,concat_ws('-',session_id,sum(flag) over(partition by session_id order by ts)) as new_session_id
        |from
        |(
        |   select
        |        *
        |       ,if(ts-lag(ts,1,ts) over(partition by session_id order by ts)>30*60*1000,1,0) as flag
        |   from tmp.mall_applog_washed
        |   where dt='${dt}'
        |) o
        |
        |
        |""".stripMargin)

    spark.close()
  }

}
