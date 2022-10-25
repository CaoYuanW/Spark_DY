package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestSample {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestMapPartitoons")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7),2)
    //sample 抽取出来的数据是否放回，抽取概率，随机数种子
    rdd.sample(true,0.2).foreach(println)
    //takeSample 抽取出来的数据是否放回，抽取几条数据，随机数种子，返回的是一个数组，如果知道返回的数据很少可以用数组盛放
    rdd.takeSample(true,2).foreach(println)
    //stop
    sc.stop()
  }
}
