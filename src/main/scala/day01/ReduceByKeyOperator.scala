package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyOperator {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf = new SparkConf()
    conf.setAppName("MapOperator")
    conf.setMaster("local")
    //程序入口对象
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Array[String]] = sc.parallelize(Seq(
      Array("a","b","c"),
      Array("d","b","c"),
      Array("e","f","c"),
    ))
    //压平
    val rdd2: RDD[String] = rdd.flatMap(arr => arr)
    rdd2.foreach(println)
    //转换
    val rdd3: RDD[(String, Int)] = rdd2.map(s => (s, 1))
    //分组聚合,相同key的value聚合
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((merge, elem) => merge + elem)
    //打印
    rdd4.foreach(println)
    //停止
    sc.stop()
  }
}
