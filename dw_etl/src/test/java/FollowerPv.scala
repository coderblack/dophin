import scala.collection.mutable.ListBuffer

case class TreeNode(url:String,children:List[TreeNode])

object FollowerPv {
  def main(args: Array[String]): Unit = {

    val x = TreeNode("x", null)
    val y = TreeNode("y", null)
    val f = TreeNode("f", List(x,y))

    val e = TreeNode("e", null)
    val c = TreeNode("c", List(e,f))

    val g = TreeNode("g", null)
    val h = TreeNode("h", null)
    val d = TreeNode("d", List(g,h))

    val b = TreeNode("b", null)

    val a = TreeNode("a", List(b,c,d))

    val buf = new ListBuffer[(TreeNode, Int)]()
    findNodeFollowerPv2(a,buf)
    println(buf.map(tp=>(tp._1.url,tp._2)))

  }


  def findNodeFollowerPv(node:TreeNode):Int = {

    var followerPv = 0
    val children: List[TreeNode] = node.children

    if(children != null){

      // 本节点的下游贡献应该等于  本节点的子节点个数 + 每个子节点的贡献量
      followerPv += children.size   // 累加本节点的子节点数
      for (child <- children) {
        followerPv += findNodeFollowerPv(child)  // 累加每个子节点的贡献量
      }
    }
    followerPv
  }


  def findNodeFollowerPv2(node:TreeNode,buf:ListBuffer[(TreeNode,Int)]):Int = {

    var followPv = 0
    val children: List[TreeNode] = node.children

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
