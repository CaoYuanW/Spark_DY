package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestCartesian {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf()
    conf.setAppName("TestCartesian")
    conf.setMaster("local")
    //SparkContext
    val sc = new SparkContext(conf)
    //读取数据源
    val rdd1: RDD[(String, Int)] = sc.parallelize(Seq(("a", 12), ("a", 13), ("b", 11), ("b", 14), ("c", 11)), 2)
    val rdd2: RDD[(String, String)] = sc.parallelize(Seq(("a", "x"), ("b", "y"), ("d", "z")), 2)
    //笛卡尔积
    rdd1.cartesian(rdd2).foreach(println)
    //stop
    sc.stop()
  }
}
