package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //获取程序配置对象
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local") //本地模式一个线程执行

    //获取程序入口对象
    val sc = new SparkContext(conf)
    //读取数据源
    //对数据进行操作
    val rdd: RDD[String] = sc.textFile("datas/words.txt")
    val rdd2: RDD[String] = rdd.flatMap(s => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(s => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((a, b) => a + b)
    rdd4.foreach(println)
    //停止行程序
    sc.stop()
  }
}
