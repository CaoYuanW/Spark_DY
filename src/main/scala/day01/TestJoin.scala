package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestJoin {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestCoalesceAndRepartition")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd1: RDD[(String, Int)] = sc.parallelize(Seq(("a", 12), ("a", 13), ("b", 11), ("b", 14), ("c", 11)), 2)
    val rdd2: RDD[(String, String)] = sc.parallelize(Seq(("a", "x"), ("b", "y"), ("d", "z")), 2)
    //join
    rdd1.join(rdd2).foreach(println)
    rdd1.leftOuterJoin(rdd2).foreach(println)
    rdd1.rightOuterJoin(rdd2).foreach(println)
    rdd1.fullOuterJoin(rdd2).foreach(println)
    //stop
    sc.stop()
  }
}
