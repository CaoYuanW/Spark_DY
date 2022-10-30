package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TestPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TestAction")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Order, Int)] = sc.parallelize(Seq(
      (Order(1, 10, 100.0), 1),
      (Order(2, 10, 80.0), 1),
      (Order(3, 12, 100.0), 1),
      (Order(2, 10, 200.0), 1),
      (Order(3, 12, 180.0), 1)
    ),2)
    //partitionBy 可以自定义分区器 按照订单的id进行分组
    rdd.partitionBy(new HashPartitioner(3))
    rdd.partitionBy(new MyPartitioner(3)).foreach(println)

    sc.stop()
  }
}


case class Order(id: Int, uid: Int, salary: Double)

class MyPartitioner(partitions: Int) extends HashPartitioner(partitions: Int) {
  override def numPartitions: Int = super.numPartitions

  override def getPartition(key: Any): Int = {
    val order: Order = key.asInstanceOf[Order]
    order.id.hashCode() % numPartitions
  }
}