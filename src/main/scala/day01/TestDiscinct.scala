package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestDiscinct {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestMapPartitoons")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7,1,2,3,4))
    //distinct 去重 可以指定分区数
    rdd.distinct().foreach(println)
    //stop
    sc.stop()
  }
}
