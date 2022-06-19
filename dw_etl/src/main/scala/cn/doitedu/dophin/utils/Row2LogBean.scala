package cn.doitedu.dophin.utils

import org.apache.spark.sql.Row

case class LogBean(
                    account           :  String,
                    app_id            :  String,
                    app_version       :  String,
                    carrier           :  String,
                    device_id         :  String,
                    device_type       :  String,
                    event_id          :  String,
                    ip                :  String,
                    latitude          :  Double,
                    longitude         :  Double,
                    net_type          :  String,
                    os_name           :  String,
                    os_version        :  String,
                    properties        :  Map[String,String],
                    release_channel   :  String,
                    resolution        :  String,
                    session_id        :  String,
                    ts                :  Long,
                    new_session_id    :  String,
                    var province      :  String = "未知" ,
                    var city          :  String = "未知" ,
                    var region        :  String = "未知" ,
                  )



object Row2LogBean {

  def row2LogBean(row:Row):LogBean = {
    LogBean(
      row.getAs[String]("account")            ,
      row.getAs[String]("app_id")             ,
      row.getAs[String]("app_version")        ,
      row.getAs[String]("carrier")            ,
      row.getAs[String]("device_id")          ,
      row.getAs[String]("device_type")        ,
      row.getAs[String]("event_id")           ,
      row.getAs[String]("ip")                 ,
      row.getAs[Double]("latitude")           ,
      row.getAs[Double]("longitude")          ,
      row.getAs[String]("net_type")           ,
      row.getAs[String]("os_name")            ,
      row.getAs[String]("os_version")         ,
      row.getAs[Map[String,String]]("properties"),
      row.getAs[String]("release_channel")    ,
      row.getAs[String]("resolution")         ,
      row.getAs[String]("session_id")         ,
      row.getAs[Long]("ts")                 ,
      row.getAs[String]("new_session_id")
    )

  }


}
