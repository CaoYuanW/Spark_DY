package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestMapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestMapPartitoons")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7),2)
    //index是元素所在分区索引号
    val rdd2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(ele => (ele, index))
    })
    //foreach
    rdd2.foreach(println)
    //stop
    sc.stop()
  }
}
