package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestAction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TestAction")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6), 3)
    val rdd1: RDD[(String, Int)] = sc.parallelize(Map(("a" -> 1), ("b" -> 2), ("c" -> 3)).toSeq, 2)
    //reduce 将rdd中所有数据聚合成一个值
    println(rdd.reduce(_ + _))
    //collect 将rdd中的数据收集到driver本地
    println(rdd.collect().mkString(","))
    //count 求rdd中元素个数
    println(rdd.count())
    //first 取rdd中的第一条数据
    println(rdd.first())
    //take 取rdd中n条数据
    println(rdd.take(3).mkString(","))
    //takeSample 随机抽样
    println(rdd.takeSample(true, 2).mkString(","))
    //takeOrdered 按照排序规则取前n条数据
    println(rdd.takeOrdered(3).mkString(","))
    //saveAsTextFile   saveAsObjectFile  saveAsSequenceFile
    //rdd.saveAsTextFile("path")
    //rdd.saveAsObjectFile("path")
    //rdd1.saveAsSequenceFile("path")
    //countByKey 计算key的元素个数
    println(rdd1.countByKey())
    //foreach
    rdd.foreach(println)
    sc.stop()
  }
}
