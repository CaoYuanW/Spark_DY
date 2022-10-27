package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestAggregate {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestAggregate")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 2, 3, 4, 5, 6), 2)
    //aggregate行动算子 分区内计算逻辑 分区间计算逻辑 每个分区拥有一份初始值 分区间计算再用一次初始值
    val result: Int = rdd.aggregate(100)((u, elem) => u + elem, (u1, u2) => u1 + u2)
    println(result)
    //stop
    sc.stop()
  }
}
