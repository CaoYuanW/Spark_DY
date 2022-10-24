package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapOperator {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf = new SparkConf()
    conf.setAppName("MapOperator")
    conf.setMaster("local")
    //程序入口对象
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[String] = sc.parallelize(List("a", "b", "c"))
    val rdd3: RDD[String] = sc.parallelize(List("1,Tom", "2,Sam"))
    //转换
    val rdd2: RDD[String] = rdd.map(_.toUpperCase())
    val rdd4: RDD[Teacher] = rdd3.map(t => {
      val datas: Array[String] = t.split(",")
      Teacher(datas(0).toInt, datas(1))
    })
    //打印
    rdd2.foreach(println)
    rdd4.foreach(println)
    //停止
    sc.stop()
  }
}

case class Teacher(id: Int, name: String)
