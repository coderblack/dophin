package cn.doitedu.dophin.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer


case class Bean(guid: Long, eventId: String, ts: Long)

/**
 * 事件归因分析
 */
object _24_DestEventAttribute {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("事件归因分析")
      .config("spark.sql.shuffle.partitions",1)
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    // 过滤事件明细表
    // -- 归因模型:model_1
    // -- 目标事件： e5,properties['p2']='v5'
    // -- 待归因事件:
    //   -- e1
    //   -- e2
    //   -- e4
    val data = spark.sql(
      """
        |
        |-- 先挑出做了目标事件的人
        |SELECT
        |    o1.guid
        |    ,o2.event_id
        |    ,o2.ts
        |FROM
        |(
        |    SELECT
        |       guid
        |    FROM tmp.log_dtl2
        |    WHERE dt='2022-02-16'
        |    AND event_id='e5' and properties['p5']='v2'
        |    GROUP BY guid
        |) o1
        |
        |JOIN
        |
        |(
        |   SELECT
        |      guid
        |      ,event_id
        |      ,ts
        |   FROM tmp.log_dtl2
        |   WHERE dt='2022-02-16' AND
        |   (
        |      ( event_id='e5'  and properties['p5']='v2')  OR
        |      event_id='e1' OR event_id='e2'  OR  event_id='e4'
        |   )
        |) o2
        |ON o1.guid=o2.guid
        |
        |""".stripMargin)

    /**
     * +----+--------+---+
     * |guid|event_id|ts |
     * +----+--------+---+
     * |2   |e1      |1  |
     * |2   |e4      |2  |
     * |2   |e3      |4  |
     *
     * |2   |e5      |5  |         -- 事件序列按 目标事件 作为分界点, 分割成多段
     * |2   |e4      |7  |         List(  List(e1,e4),  List(e5,e4),  List(e1)  )
     * |2   |e3      |9  |         -- 然后对每一段中的归因事件按照不同算法进行打分即可
     *
     * |2   |e1      |9  |
     * |2   |e3      |9  |
     * +----+--------+---+
     */
    val rdd = data.rdd.map(row => {
      val guid = row.getAs[Long]("guid")
      val eventId = row.getAs[String]("event_id")
      val ts = row.getAs[Long]("ts")
      Bean(guid, eventId, ts)
    })


    /**
     * --模型id,用户,待归因事件,计算策略,归因权重
     * modle1,g01,e1,首次触点归因策略,100.0
     * modle1,g01,e4,末次触点归因策略,100.0
     * modle1,g01,e1,线性归因策略    ,50.0
     * modle1,g01,e4,线性归因策略    ,50.0
     * modle1,g01,e1,位置归因策略    ,20.0
     * modle1,g01,e4,位置归因策略    ,80.0
     * modle1,g01,e1,时间衰减归因策略,33.3
     * modle1,g01,e4,时间衰减归因策略,66.6
     */

    // 首次触点归因分析
    val destEventId = "e5"
    val result = firstOrLastAttr(spark, rdd, false,destEventId).selectExpr(" 'model_1' as model_id ", "guid", "attr_event", " '首次触点归因' as attr_alg ", "attr_weight")
      .unionAll(
        firstOrLastAttr(spark, rdd, true,destEventId).selectExpr(" 'model_1' as model_id ", "guid", "attr_event", " '末次触点归因' as attr_alg ", "attr_weight")
      )
      .unionAll(
        linearAttr(spark, rdd,destEventId).selectExpr(" 'model_1' as model_id ", "guid", "attr_event", " '线性归因' as attr_alg ", "attr_weight")
      )
      .unionAll(
        timeDecayAttr(spark, rdd,destEventId).selectExpr(" 'model_1' as model_id ", "guid", "attr_event", " '时间衰减归因' as attr_alg ", "attr_weight")
      )


    result.show(100,false)
    spark.close()
  }

  /**
   * 首次（末次）触点归因
   *
   * @param rdd
   * @return
   */
  def firstOrLastAttr(spark: SparkSession, rdd: RDD[Bean], reverse: Boolean = false,destEventId:String): DataFrame = {

    // (用户,归因事件,归因权重)
    val resRdd: RDD[(Long, String, Int)] = rdd
      .groupBy(_.guid)
      .flatMap(tp => {
        // 先对一个人的事件序列按时间先后排序
        val eventSeq: Seq[Bean] = tp._2.toList.sortBy(_.ts)


        // 将该用户的事件序列按“目标事件”分割成多段
        val bigList = eventSeqSplit(eventSeq,destEventId)

        // 输出结果
        bigList.map(lst => {
          if (reverse) { // 如果reverse=true,说明要求末次触点归因
            (tp._1, lst.reverse.head, 100)
          } else {
            (tp._1, lst.head, 100)
          }
        })
      })

    import spark.implicits._

    resRdd.toDF("guid", "attr_event", "attr_weight")
  }


  /**
   * 线性归因策略
   *
   * @param spark
   * @param rdd
   * @return
   */
  def linearAttr(spark: SparkSession, rdd: RDD[Bean],destEventId:String): DataFrame = {

    val resRdd: RDD[(Long, String, Double)] = rdd
      .groupBy(_.guid)
      .flatMap(tp => {
        val eventSeq = tp._2.toList.sortBy(_.ts)

        // 切割事件序列
        //  List(  List(e1,e4),  List(e5,e4),  List(e1)  )
        val bigList = eventSeqSplit(eventSeq,destEventId)

        bigList.flatMap(lst => {
          val weight = 100.0 / lst.size
          lst.map(e => (tp._1, e, weight))
        })
      })

    import spark.implicits._
    resRdd.toDF("guid", "attr_event", "attr_weight")

  }


  /**
   * 时间衰减归因策略
   *
   * @param spark
   * @param rdd
   * @return
   */
  def timeDecayAttr(spark: SparkSession, rdd: RDD[Bean],destEventId:String): DataFrame = {

    val resRdd: RDD[(Long, String, Double)] = rdd
      .groupBy(_.guid)
      .flatMap(tp => {
        val guid = tp._1
        val eventSeq = tp._2.toList.sortBy(_.ts)

        // 切割事件序列
        //  List(  List(e1,e4),  List(e5,e4),  List(e1)  )
        val bigList = eventSeqSplit(eventSeq,destEventId)

        // 打分
        bigList.flatMap(lst => {

          // 每个事件  挂上  脚标+1
          val tmp: Seq[(String, Int)] = lst.zipWithIndex
            .map(tp => (tp._1, tp._2 + 1))

          val fenMu = tmp.map(_._2).sum.toDouble

          tmp.map(tp => (guid, tp._1, tp._2 * 100 / fenMu))

        })
      })

    import spark.implicits._
    resRdd.toDF("guid", "attr_event", "attr_weight")

  }


  /**
   * 工具方法：切割事件序列
   *
   * @param eventSeq
   * @return
   */
  def eventSeqSplit(eventSeq: Seq[Bean],destEvent:String): ListBuffer[ListBuffer[String]] = {
    val bigList = new ListBuffer[ListBuffer[String]]
    var smallList = new ListBuffer[String]

    // 遍历事件序列
    for (bean <- eventSeq) {
      // 判断,是否是目标事件,如果不是则添加到小list中
      if (!bean.eventId.equals(destEvent)) {
        smallList += bean.eventId
      } else {
        // 如果是目标事件,则把当前小list添加到大list，并创建一个新的小list
        bigList += smallList
        smallList = new ListBuffer[String]
      }
    }

    // 大list中有可能存在空的小list,先过滤一下
    bigList.filter(_.nonEmpty)
  }


}
