package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestAggregateByKey {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestAggregateByKey")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 2), ("a", 1), ("b", 3), ("c", 4), ("c", 6), ("d", 2), ("a", 2)),2)
    //aggregateByKey 分区内聚合逻辑 分区间聚合逻辑 初始值每个分区每种key初始化一次
    rdd.aggregateByKey(100)((merge, element) => merge + element, (merge1, merge2) => merge1 + merge2).foreach(println)
    //stop
    sc.stop()
  }
}
