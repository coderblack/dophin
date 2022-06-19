package cn.doitedu.dophin.etl

import cn.doitedu.dophin.utils.TreeFollowPvUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * 页面受访分析-主题宽表生成任务
 * 运行本任务，需要提前拥有如下表：
 *      行为日志明细表 dwd.mall_applog_detail
 *      会话聚合信息表 dws.mall_app_ses_agr_d
 *      日活表 dws.mall_app_dau
目标表建表语句：
create table dws.mall_app_pv_wide(
    url              string
    ,session_id      string
    ,guid            bigint
    ,stay_long       bigint
    ,ref_url         string
    ,ts              bigint
    ,is_new          int
    ,follow_pv       int
    ,is_exit         int
    ,is_enter        int
    ,ref_type        string
)
partitioned by (dt string)
stored as orc
tblproperties('orc.compress'='snappy')
;
 生成的目标表示例： dws.mall_app_pv_wide
    +-----------------------------+---------------+-------------+--------------+--------------------------+----------------+-----------+--------------+------------+-------------+-------------+-------------+
    |            t.url            | t.session_id  |   t.guid    | t.stay_long  |        t.ref_url         |      t.ts      | t.is_new  | t.follow_pv  | t.is_exit  | t.is_enter  | t.ref_type  |    t.dt     |
    +-----------------------------+---------------+-------------+--------------+--------------------------+----------------+-----------+--------------+------------+-------------+-------------+-------------+
    | /students/stu0462.html      | lajmqcdp      | 70287       | 32324        | /students/stu0641.html   | 1645018204352  | 1         | 3            | 0          | 1           | 站内跳转        | 2022-02-16  |
    | /courses/azkaban/c034.html  | fkzaextn      | 79683       | NULL         | /contacts/con0347.html   | 1645018204380  | 1         | 0            | 1          | 1           | 站内跳转        | 2022-02-16  |
    | /teachers/tea0581.html      | yaqxmxhz      | 54751       | 103595       | /shares/sha0346.html     | 1645018204876  | 1         | 1            | 0          | 1           | 站内跳转        | 2022-02-16  |
    | /contacts/con0828.html      | ypabvfnw      | 47323       | NULL         | /shares/sha0754.html     | 1645018205059  | 1         | 0            | 1          | 1           | 站内跳转        | 2022-02-16  |
    | /students/stu0554.html      | sjmklhnb      | 1000001314  | NULL         | /contacts/con0770.html   | 1645018205184  | 1         | 0            | 1          | 1           | 站内跳转        | 2022-02-16  |

 */


case class PageView(url: String, sessionId: String, guid: Long, ts: Long, var refUrl: String)

case class TreeNode(pageView: PageView, children: ListBuffer[TreeNode])

object _12_PageViewStatistic {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("页面受访分析-主题宽表生成任务")
      .master("local")
      .config("spark.sql.shuffle.partitions", 4)
      .enableHiveSupport()
      .getOrCreate()

    // 读出页面访问明细数据
    val pageDetail = spark.sql(
      """
        |
        |select
        |    properties['url'] as page_url,
        |    session_id,
        |    guid,
        |    ts,
        |    properties['refUrl'] as ref_url
        |from dwd.mall_applog_detail
        |where dt='2022-02-16'
        |and event_id = 'pageView'
        |
        |""".stripMargin)

    // 测试用数据
    /*val pageDetail = spark.sql(
      """
        |
        |select  '/a' as page_url, 's01' as session_id , cast(1  as bigint) as guid,  cast(1 as bigint) as ts , null as ref_url
        |union all
        |select  '/b' as page_url, 's01' as session_id , 1 as guid, 2 as ts,  '/a' as ref_url
        |union all
        |select  '/c' as page_url, 's01' as session_id , 1 as guid, 3 as ts,  '/b' as ref_url
        |union all
        |select  '/x' as page_url, 's01' as session_id , 1 as guid, 4 as ts,  '/b' as ref_url
        |union all
        |select  '/d' as page_url, 's01' as session_id , 1 as guid, 5 as ts,  'http://www.baidu.com/search?kkk=yyy' as ref_url
        |union all
        |select  '/e' as page_url, 's01' as session_id , 1 as guid, 6 as ts,  '/d' as ref_url
        |union all
        |select  '/f' as page_url, 's01' as session_id , 1 as guid, 7 as ts,  '/d' as ref_url
        |union all
        |select  '/w' as page_url, 's01' as session_id , 1 as guid, 8 as ts,  '/f' as ref_url
        |union all
        |select  '/m' as page_url, 's01' as session_id , 1 as guid, 9 as ts,  '/f' as ref_url
        |""".stripMargin)*/


    val pageDetailRDD = pageDetail.rdd.map(row => {
      // t.page_url,t.session_id,t.guid,t.ts,t.ref_url
      val url = row.getAs[String]("page_url")
      val sessionId = row.getAs[String]("session_id")
      val guid = row.getAs[Long]("guid")
      val ts = row.getAs[Long]("ts")
      val refUrl = row.getAs[String]("ref_url")

      PageView(url, sessionId, guid, ts, refUrl)
    })

    // 按相同会话id分组
    val grouped: RDD[(String, Iterable[PageView])] = pageDetailRDD.groupBy(pageView => pageView.sessionId)

    // 核心算法逻辑，将每个会话的一组浏览事件，变成 多棵 树
    val treeRDD: RDD[TreeNode] = grouped.flatMap(tp => {
      // 一个会话中的所有页面浏览事件
      val pageViewlst: List[PageView] = tp._2.toList.sortBy(pageView => pageView.ts)
      /**
       * List(
       * a, \n ,1
       * b, a  ,2
       * c, a  ,3
       * d, b  ,4
       * a, b  ,5
       * e, a  ,6
       * )
       */
      // 用于装多棵树的list, 里面装的每个list都是一棵树
      val treeList = new ListBuffer[ListBuffer[TreeNode]]

      // 用于装一棵树的list,里面装的每个TreeNode都是树的一个节点
      var nodesList: ListBuffer[TreeNode] = null

      // 开始遍历事件列表
      for (pageView <- pageViewlst) {

        // 将当前的一条数据构造成一个TreeNode,子节点列表为空
        val node = TreeNode(pageView, new ListBuffer[TreeNode]())

        // 如果是第一个页面，或者是站外过来的，则创建一棵新的树
        if (nodesList == null || pageView.refUrl == null || pageView.refUrl.startsWith("http://")) {
          // 创建一棵新的树list
          nodesList = new ListBuffer[TreeNode]

          // 把构造的当前节点，添加到树中
          nodesList += node

          // 把树放到一个树List中去
          treeList += nodesList
        }
        // 否则不创建新的树
        else {
          // 把构造的当前节点，添加到树中
          nodesList += node

          // 将当前节点挂载到正确的父节点下
          if (pageView.refUrl != null && pageView.refUrl.startsWith("/")) {
            // 去查找之前构造好的父节点，把当前节点添加为它的子节点
            val maybeFather: Option[TreeNode] = nodesList.reverse.find(node => node.pageView.url.equals(pageView.refUrl))
            if (maybeFather.isDefined) {
              // 如果找到了父节点，则给该父节点添加当前节点作为一个子节点
              maybeFather.get.children += node
            } else {
              // 如果没有找到父节点，这是模拟数据出现的矛盾现象
              // 偷个懒，直接将它挂在最后的一个节点上
              if (nodesList.size > 1)
                nodesList(nodesList.size - 2).children += node
            }
          }
        }
      }

      // 最后返回树集合中的每棵树的 根节点
      treeList.map(lst => lst.head)
    })

    /**
     * 对rdd中的每一颗树，调另外一个算法： 求一棵树中所有节点的下游贡献量
     */
    import spark.implicits._
    val tmp = treeRDD.flatMap(tree => {
      val buf = new ListBuffer[(TreeNode, Int)]
      TreeFollowPvUtil.findNodeFollowerPv2(tree, buf)

      buf.map(tp => {
        val view = tp._1.pageView
        (view.guid, view.sessionId, view.url, view.refUrl, view.ts, tp._2)
      })
    }).toDF("guid","session_id","url","ref_url","ts","follow_pv")
    // tmp.show(100,false)
    /**
     * +----+----------+---+-------+---+---------+
       |guid|session_id|url|ref_url|ts |follow_pv|
       +----+----------+---+-------+---+---------+
       |1   |s01       |/c |/b     |3  |0        |
       |1   |s01       |/x |/b     |4  |0        |
       |1   |s01       |/b |/a     |2  |2        |
       |1   |s01       |/a |null   |1  |3        |
       |1   |s01       |/e |/d     |6  |0        |
       |1   |s01       |/w |/f     |8  |0        |
       |1   |s01       |/m |/f     |9  |0        |
       |1   |s01       |/f |/d     |7  |2        |
       |1   |s01       |/d |null   |5  |4        |
       +----+----------+---+-------+---+---------+
     */

    // 利用sql,来生成其他的目标字段,并将结果最终插入hive的目标表
    tmp.createTempView("tmp")
    spark.sql(
      """
        |insert into table dws.mall_app_pv_wide partition(dt='2022-02-16')
        |select
        |   url
        |   ,tmp.session_id
        |   ,tmp.guid
        |   ,lead(tmp.ts,1,t2.ses_end) over(partition by tmp.session_id order by tmp.ts) - ts as stay_long
        |   ,tmp.ref_url
        |   ,tmp.ts
        |   ,if(t3.guid is not null,1,0)  as is_new
        |   ,tmp.follow_pv
        |   ,if(row_number() over(partition by tmp.session_id order by tmp.ts desc)>1,0,1)  as is_exit
        |   ,if(row_number() over(partition by tmp.session_id order by tmp.ts asc)>1,0,1)  as is_enter
        |   ,case
        |       when tmp.ref_url is null then '直接访问'
        |       when tmp.ref_url like '/%' then '站内跳转'
        |       when parse_url(tmp.ref_url,'HOST') in ('www.baidu.com','www.sogou.com','www.biying.com','so.360.com')  then parse_url(tmp.ref_url,'HOST')
        |       else '外链访问'
        |    end as ref_type
        |from tmp
        |LEFT JOIN
        | (
        |  SELECT
        |    session_id
        |    ,ses_end
        |   FROM dws.mall_app_ses_agr_d
        |   WHERE dt='2022-02-16'
        |  ) t2
        |ON tmp.session_id=t2.session_id
        |LEFT JOIN
        | (
        |   SELECT
        |      guid
        |   FROM dws.mall_app_dau
        |   WHERE dt='2022-02-16' AND is_new=1
        | ) t3
        |ON tmp.guid=t3.guid
        |sort by ts
        |
        |""".stripMargin)

    spark.close()

  }
}
