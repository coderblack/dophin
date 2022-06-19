package cn.doitedu.dophin.etl

import ch.hsr.geohash.GeoHash
import cn.doitedu.dophin.utils.{LogBean, Row2LogBean}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import java.io.File
import java.net.URI
import java.util.Properties

/**
 * app日志地理位置集成
 * create table tmp.mall_applog_area(
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
    ,province            string
    ,city                string
    ,region              string
)
partitioned by (dt string)
stored as orc
tblproperties('orc.compress'='snappy')
;
 *
 *
 */

case class GeoArea(geohash: String, province: String, city: String, region: String)


object ApplogRegionProcess {

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
      .appName("app日志地理位置集成")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    // 读取geohash地理位置参考表
    val geoRef = spark.read.table("dim.ref_geo").where("geohash is not null")
    // 将参考表数据转成case class，并收集到driver端的单机hashmap集合中
    val areaMap = geoRef.rdd.map({
      case Row(geohash: String, province: String, city: String, region: String) => (geohash, GeoArea(geohash, province, city, region))
    }).collectAsMap()
    // 将参考数据hashmap广播出去
    val bc1 = spark.sparkContext.broadcast(areaMap)

    // 加载配置参数，获取hdfs的地址
    val props = new Properties()
    props.load(ApplogRegionProcess.getClass.getClassLoader.getResourceAsStream("db.properties"))

    // 从hdfs上读取ip字典库文件到一个字节数组
    val fs = FileSystem.get(new URI(props.getProperty("hdfs.path") + "/ip2region/ip2region.db"), new Configuration(), "root")
    val path = new Path("/ip2region/ip2region.db")
    val fSDataInputStream = fs.open(path)

    // 获取ip2region.db文件的字节长度
    val status = fs.getFileStatus(path)

    // 准备一个字节数组，并用io工具，将文件流全部读入这个字节数组中
    val ip2RegionDbBytes: Array[Byte] = new Array[Byte](status.getLen.toInt)
    IOUtils.readFully(fSDataInputStream,ip2RegionDbBytes)

    // 用单机io来读取ip2region的参考字典库文件
    // val ip2RegionDbBytes: Array[Byte] = FileUtils.readFileToByteArray(new File("data/ip2region/ip2region.db"))

    // 把ip字典库字节数组广播出去
    val bc2 = spark.sparkContext.broadcast(ip2RegionDbBytes)


    // 读取session分割之后的日志表
    val applog = spark.read.table("tmp.mall_applog_session_split").where(s"dt='${dt}'")

    // 进行地理位置的查询处理
    val areaApplog: RDD[LogBean] = applog.rdd.mapPartitions(iter => {

      val areaDict = bc1.value

      val config = new DbConfig()
      val ip2RegionBytes = bc2.value
      val searcher = new DbSearcher(config, ip2RegionBytes)

      iter.map(row => {

        var province = "未知"
        var city = "未知"
        var region = "未知"

        // 将一条日志数据  转成  LogBean对象
        val logBean = Row2LogBean.row2LogBean(row)

        // 从LogBean对象中抽取gps坐标，去字典hashmap中查询地理位置
        val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(logBean.latitude, logBean.longitude, 5)
        val maybeArea = areaDict.get(geoHashCode)

        // 如果通过gps查到了地理位置信息
        if (maybeArea.isDefined) {
          val area: GeoArea = maybeArea.get

          // 赋值
          province = area.province
          city = area.city
          region = area.region
        }
        // 如果通过gps没有查到,则用ip地址去查
        else {
          val ip = logBean.ip
          // 用ip2region工具去查
          // 中国|0|上海|上海市|电信
          val regionStr = searcher.memorySearch(ip).getRegion
          val splits = regionStr.split("\\|")
          if (splits.length >= 5 && !splits(3).equals("内网IP")) {

            // 赋值
            province = splits(2)
            city = splits(3)
          }
        }


        // 将省市区结果，放入logBean，返回
        logBean.province = province
        logBean.city = city
        logBean.region = region

        logBean

      })

    })

    // 将结果写入一个临时parquet文件
    import spark.implicits._
    // 将地理位置集成好的数据，存入临时表:
    areaApplog.toDS().createTempView("tmp")
    spark.sql(
      s"""
        |
        |insert into table tmp.mall_applog_area partition(dt='${dt}')
        |select * from tmp
        |
        |""".stripMargin)


    spark.close()
  }

}
