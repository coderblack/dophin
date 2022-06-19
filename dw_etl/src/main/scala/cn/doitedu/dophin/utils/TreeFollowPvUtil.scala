package cn.doitedu.dophin.utils

import cn.doitedu.dophin.etl.TreeNode

import scala.collection.mutable.ListBuffer

object TreeFollowPvUtil {

  def findNodeFollowerPv2(node:TreeNode,buf:ListBuffer[(TreeNode,Int)]):Int = {

    var followPv = 0
    val children: ListBuffer[TreeNode] = node.children

    if(children != null){

      // 本节点的下游贡献应该等于  本节点的子节点个数 + 每个子节点的贡献量
      followPv += children.size   // 累加本节点的子节点数

      for (child <- children) {
        followPv += findNodeFollowerPv2(child,buf)  // 累加子节点的贡献量
      }
    }

    // 把当前节点算出来的结果，存入buf中
    buf += ((node,followPv))
    followPv
  }

}
