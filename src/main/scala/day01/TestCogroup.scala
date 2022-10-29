package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestCogroup {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf()
    conf.setAppName("TestCogroup")
    conf.setMaster("local")
    //SparkContext
    val sc = new SparkContext(conf)
    //读取数据源
    val rdd1: RDD[(String, Int)] = sc.parallelize(Seq(("a", 12), ("a", 13), ("b", 11), ("b", 14), ("c", 11)), 2)
    val rdd2: RDD[(String, String)] = sc.parallelize(Seq(("a", "x"), ("b", "y"), ("d", "z")), 2)
    //coGrouop 协同分组 相同key一组，第一个迭代器里面存放rdd1的一组key的value，第一个迭代器里面存放rdd2的一组key的value，
    rdd1.cogroup(rdd2).foreach(println)
    //stop
    sc.stop()
  }
}
