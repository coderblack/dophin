package cn.doitedu.dophin.etl

import org.apache.spark.sql.SparkSession

object _4_ApplogGuid {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |usage:必须传入1个参数
          |  参数1:待处理的日期，如 2022-02-16
          |""".stripMargin)
      sys.exit(1)
    }

    val dt: String = args(0)


    val spark = SparkSession.builder()
      .appName("app日志地理位置集成")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    // 先读取hive中的 经过了清洗、过滤、规范化、session分割、地理位置集成之后的临时表
    val logTable = spark.read.table("tmp.mall_applog_area").drop("dt").where(s"dt='${dt}'")

    logTable.cache()

    // 将日志数据分为两部分：带账号信息；不带账号信息
    val hasAccount = logTable.where(" trim(account)!='' and account is not null")
    val noAccount = logTable.where(" trim(account)='' or account is null")

    // 为不带账号的数据，去“设备账号绑定关系表中”找账号
    // 1. 加工“设备账号绑定关系表”,得到表中的每一个device_id所绑定的权重最大的 账号
    val deviceBind = spark.sql(
      s"""
        |select
        |   device_id,
        |   account,
        |   row_number() over(partition by device_id order by weight desc,last_login desc) as rn
        |from dws.mall_app_device_account_bind
        |where dt='${dt}'
        |
        |""".stripMargin).where("rn=1")
    // 2. 为不带账号的数据，去“设备账号绑定关系表中”找账号
    noAccount.createTempView("noAccount")
    deviceBind.createTempView("device_bind")
    val tmp2 = spark.sql(
      """
        |
        |select
        |  -- 取绑定关系中的账号
        | nvl(t2.account,null)  as account
        | ,t1.app_id
        | ,t1.app_version
        | ,t1.carrier
        | ,t1.device_id
        | ,t1.device_type
        | ,t1.event_id
        | ,t1.ip
        | ,t1.latitude
        | ,t1.longitude
        | ,t1.net_type
        | ,t1.os_name
        | ,t1.os_version
        | ,t1.properties
        | ,t1.release_channel
        | ,t1.resolution
        | ,t1.session_id
        | ,t1.ts
        | ,t1.new_session_id
        | ,t1.province
        | ,t1.city
        | ,t1.region
        |from noAccount t1 left join device_bind t2 on t1.device_id = t2.device_id
        |
        |""".stripMargin)

    tmp2.cache()

    // 将上面的tmp2数据，再分成两部分： 找到了账号的，没找到账号
    val findAccount = tmp2.where("account is not null") // 找到了账号的
    val notFindAccount = tmp2.where("account is null") // 没找到账号的


    // 本来就有账号 union all  找到了账号的  ,去关联“用户注册信息表"，来得到user_id
    hasAccount.unionAll(findAccount).createTempView("hasAccount")
    val part1 = spark.sql(
      """
        |select
        |  t2.user_id,
        |  t1.*
        |from hasAccount  t1
        |join
        |dwd.user_reg_info t2
        |on  t1.account=t2.account
        |
        |""".stripMargin)

    // 没找到账号的数据  , 去关联"空设备id映射表" ，得到user_id
    notFindAccount.createTempView("notFindAccount")
    val part2 = spark.sql(
      """
        |
        |select
        |   t2.user_id,
        |   t1.*
        |from notFindAccount t1 join dws.mall_app_device_tmpid t2 on t1.device_id=t2.device_id
        |
        |""".stripMargin)

    // 最后，把这两部分都拥有了user_id的数据union到一起，就是完整结果
    part1.createTempView("part1")
    part2.createTempView("part2")
    spark.sql(
      s"""
        |
        |insert into table dwd.mall_applog_detail partition(dt='${dt}')
        |select  * from part1
        |union all
        |select * from part2
        |
        |""".stripMargin)


    spark.close()

  }

}
