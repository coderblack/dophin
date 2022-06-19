package cn.doitedu.dophin.etl

import org.apache.spark.sql.SparkSession

/**
 * app端日志数据，清洗过滤计算
 *
 * 要先建好一个临时表：存放清洗之后数据用
 * create table tmp.mall_applog_washed(
    account            string
    ,app_id              string
    ,app_version         string
    ,carrier            string
    ,device_id           string
    ,device_type         string
    ,event_id            string
    ,ip                 string
    ,latitude           double
    ,longitude          double
    ,net_type            string
    ,os_name             string
    ,os_version          string
    ,properties         map<string,string>
    ,release_channel     string
    ,resolution         string
    ,session_id          string
    ,ts                 bigint
)
partitioned by (dt string)
stored as orc
tblproperties('orc.compress'='snappy')
;
 *
 *
 */
object _1_ApplogWash {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
        """
          |usage:必须传入2个参数
          |  参数1:待处理的日期，如 2022-02-16
          |  参数2:待处理日期的后一天
          |""".stripMargin)
      sys.exit(1)
    }

    val dt:String = args(0)
    val dt_next:String = args(1)


    val spark = SparkSession.builder()
      //.config("spark.sql.shuffle.partitions",2)
      .appName("app端日志数据，清洗过滤计算")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      s"""
        |insert into table tmp.mall_applog_washed partition(dt='${dt}')
        |select
        |    if(trim(account)='',null,account) as account
        |    ,appid                 as  app_id
        |    ,appversion            as  app_version
        |    ,carrier               as  carrier
        |    ,deviceid              as  device_id
        |    ,devicetype            as  device_type
        |    ,eventid               as  event_id
        |    ,ip                    as  ip
        |    ,latitude              as  latitude
        |    ,longitude             as  longitude
        |    ,nettype               as  net_type
        |    ,osname                as  os_name
        |    ,osversion             as  os_version
        |    ,properties            as  properties
        |    ,releasechannel        as  release_channel
        |    ,resolution            as  resolution
        |    ,sessionid             as  session_id
        |    ,`timestamp`           as  ts
        |
        |from ods.mall_app_log
        |where dt='${dt}'
        |-- 关键字段不能缺失
        |and eventid is not null and trim(eventid) != ''
        |and sessionid is not null and trim(sessionid)!=''
        |and deviceid is not null and trim(deviceid) != ''
        |and properties is not null
        |-- 数据时间必须在指定的日期时间之内
        |and timestamp >= unix_timestamp('${dt}', 'yyyy-MM-dd')*1000 and timestamp < unix_timestamp('${dt_next}', 'yyyy-MM-dd')*1000
        |
        |""".stripMargin)


    spark.close()
  }
}
