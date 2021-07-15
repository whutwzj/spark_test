import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotTOP10Category {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //      .setMaster("yarn")
      .setMaster("local")
      .setAppName("req1")
    val sc = new SparkContext(sparkConf)

    // /tmp/xxx/.txt

    val actionRdd = sc.textFile("datas/user_visit_action.txt")

    //统计品类的 点击、下单、支付数量
    val clickActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    val orderActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )


    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    val payActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    //排序取前十
    //(品类ID，点击数量)，(品类ID，下单量)，(品类ID，支付数量)
    //要得到(品类ID，(点击，下单，支付数量))
    //可以通过join，zip，cogroup
    //join要对相同key进行join，如果点击A100次，下单为0则下单没有A这个key，无法join
    //zip是把两个RDD组合成kv键值对的形式
    //cogroup将两个RDD相同k的value整合在一起
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val totalCountRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }

        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }

        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }
    //进行元组排序，先比较第一个，再第二个，以此类推
    //_._2 按照每一行的第二个变量来排
    val resultRDD = totalCountRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)


    sc.stop()
  }
}
