package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestCoalesceAndRepartition {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestCoalesceAndRepartition")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 2, 3, 4, 5, 6), 2)
    //coalesce shuffle=true时，可以增加或减少分区，shuffle=false时，只能减少分区
    val rdd2: RDD[Int] = rdd.coalesce(3,true)
    println(rdd2.partitions.size)
    val rdd3: RDD[Int] = rdd.coalesce(1,true)
    println(rdd3.partitions.size)
    val rdd4: RDD[Int] = rdd.coalesce(1,false)
    println(rdd4.partitions.size)
    //repartition 可以增加或减少分区，也可以减少分区
    val rdd5: RDD[Int] = rdd.repartition(3)
    println(rdd5.partitions.size)
    val rdd6: RDD[Int] = rdd.repartition(1)
    println(rdd6.partitions.size)
    //stop
    sc.stop()
  }
}
