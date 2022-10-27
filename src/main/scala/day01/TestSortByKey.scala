package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestSortByKey {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestSortByKey")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 2), ("a", 1), ("b", 3), ("c", 4), ("c", 6), ("d", 2), ("a", 2)),2)
    //sortByKey 每个分区内有序 全局也有序 有的是RangePartitioner
    rdd.sortByKey().foreach(println)
    //stop
    sc.stop()
  }
}
