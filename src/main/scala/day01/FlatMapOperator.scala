package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object FlatMapOperator {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf = new SparkConf()
    conf.setAppName("MapOperator")
    conf.setMaster("local")
    //程序入口对象
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[String] = sc.parallelize(Array("a b c","d e f","g h i"))
    val rdd3: RDD[List[Int]] = sc.parallelize(Seq(
      List(1, 2, 3),
      List(4, 5, 6),
      List(7, 8, 9)
    ))
    //FlatMap 输出是一个可以迭代的东西,会依次迭代出每一个元素
    val rdd2: RDD[String] = rdd.flatMap(s=>s.split(" "))
    val rdd4: RDD[Int] = rdd3.flatMap(list=>list)
    //打印
    rdd2.foreach(println)
    rdd4.foreach(println)
    //停止
    sc.stop()
  }
}
