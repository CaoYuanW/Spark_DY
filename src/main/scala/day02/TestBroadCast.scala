package day02

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestBroadCast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TestAction")
    val sc = new SparkContext(conf)

    val map: Map[Int, String] = Map((1->"tom"),(2->"jack"),(3->"Sam"))

    val rdd: RDD[(Int, String)] = sc.parallelize(Seq(
      (1, "red"),
      (2, "blue"),
      (3, "green")
    ))

    //广播变量，将数据广播出去，每个Executor进程一份，效率较高，使用BitTorrent协议。
    val bc: Broadcast[Map[Int, String]] = sc.broadcast(map)

    rdd.map(tp=>{
      val color: String = bc.value.get(tp._1).get
      (tp._1,tp._2,color)
    }).foreach(println)

    sc.stop()
  }
}
