package cn.doitedu.dophin.utils

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * 一次性操作：生成geohash地理位置参考字典表
 * 先建好维表层的库：  create database dim;
 */
object GeohashReferenceDict {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.config("spark.sql.shuffle.partitions",2)
      .appName("gps参考坐标数据加工成geohash字典")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    val props = new Properties()
    props.load(GeohashReferenceDict.getClass.getClassLoader.getResourceAsStream("db.properties"))

    val df = spark.read.jdbc(props.getProperty("url"), "t_md_areas", props)
    df.createTempView("df")

    // 定义一个将gps坐标转成geohash码的函数
    val gps2GeoHash = (lat:Double,lng:Double)=>GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5)
    spark.udf.register("gps2geo",gps2GeoHash)


    val res = spark.sql(
      """
        |
        |select
        |   geohash,
        |   province,
        |   city,
        |   region
        |from (
        |    select
        |        gps2geo(l4.bd09_lat,l4.bd09_lng) as geohash,
        |        l1.areaname as province,
        |        l2.areaname as city,
        |        l3.areaname as region
        |    from df l4 join df l3 on l4.parentid=l3.id and l4.level=4
        |               join df l2 on l3.parentid=l2.id
        |               join df l1 on l2.parentid=l1.id
        |
        |    UNION ALL
        |
        |    select
        |        gps2geo(l3.bd09_lat,l3.bd09_lng) as geohash,
        |        l1.areaname as province,
        |        l2.areaname as city,
        |        l3.areaname as region
        |    from df l3 join df l2 on l3.parentid=l2.id and l3.level=3
        |               join df l1 on l2.parentid=l1.id
        |) o
        |group by geohash,province,city,region
        |
        |""".stripMargin)

    res.write.saveAsTable("dim.ref_geo")

    spark.close()

  }
}
