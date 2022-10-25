package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestGroupByKey {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestMapPartitoons")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 2),("a", 1),("b", 3),("c", 4),("c", 6),("d", 2),("a", 2)))
    //groupByKey 相同的key一组 value是迭代器value的集合
    val rdd2: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //map
    rdd2.map(tp=>{
      (tp._1,tp._2.sum)
    }).foreach(println)
    //stop
    sc.stop()
  }
}
