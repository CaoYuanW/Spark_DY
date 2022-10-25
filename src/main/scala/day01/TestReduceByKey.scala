package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestReduceByKey {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestMapPartitoons")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 2),("a", 1),("b", 3),("c", 4),("c", 6),("d", 2),("a", 2)))
    //reduceByKey有局部聚合功能 效率较高
    rdd.reduceByKey((merge,elem)=>merge+elem).foreach(println)
    //stop
    sc.stop()
  }
}
