package day01

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 求每个城市kill和guanya
 */
object TestExer {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf()
    conf.setAppName("TestExer")
    conf.setMaster("local")
    //SparkContext
    val sc = new SparkContext(conf)
    //读取数据源
    val rdd1: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\one.txt")
    val rdd2: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\two.txt")
    //map
    val maped1: RDD[(Int, (String, Int))] = rdd1.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0).toInt, (arr(1), arr(2).toInt))
    })
    val maprd2: RDD[(Int, (String, Int, String))] = rdd2.map(line => {
      val jsonObject: JSONObject = JSON.parseObject(line)
      (
        jsonObject.getIntValue("id"),
        (jsonObject.getString("name"),
        jsonObject.getIntValue("age"),
        jsonObject.getString("city")
      ))
    })
    //join
    val joined: RDD[(Int, ((String, Int), (String, Int, String)))] = maped1.join(maprd2)
    //map
    val mapred: RDD[(Int, String, Int, String, Int, String)] = joined.map(tp=>(tp._1,tp._2._1._1,tp._2._1._2,tp._2._2._1,tp._2._2._2,tp._2._2._3))
    //groupBy
    val grouped: RDD[(String, Iterable[(Int, String, Int, String, Int, String)])] = mapred.groupBy(tp=>tp._6)
    //map
    grouped.map(tp=>{
      val kill: Int = tp._2.filter(d=>d._2.equals("kill")).map(d=>d._3).sum
      val guanya: Int = tp._2.filter(d=>d._2.equals("guanya")).map(d=>d._3).sum
      (tp._1, kill,guanya)
    }).foreach(println)
    //stop
    sc.stop()
  }
}
