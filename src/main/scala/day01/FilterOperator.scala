package day01

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object FilterOperator {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf = new SparkConf()
    conf.setAppName("MapOperator")
    conf.setMaster("local")
    //程序入口对象
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\filter.txt")
    //转换
    val rdd2: RDD[(Int, String)] = rdd.map(s => {
      val datas: Array[String] = s.split(",")
      (datas(0).toInt, s)
    })
    //过滤
    val rdd3: RDD[(Int, String)] = rdd2.filter(t => t._1 % 2 == 0)
    //打印
    rdd3.foreach(println)

    //读取数据
    val rdd4: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\json.txt")
    //转换过滤
    val rdd5: RDD[Person] = rdd4.map(s => {
      try {
        val obj: JSONObject = JSON.parseObject(s)
        val id: Int = obj.getIntValue("id")
        val age: Int = obj.getIntValue("age")
        val grade: Int = obj.getIntValue("grade")
        Person(id, age, grade)
      } catch {
        case e: Exception => null
      }
    }).filter(person => person != null)
    //打印
    rdd5.foreach(println)
    //停止
    sc.stop()
  }
}

//Person
case class Person(id: Int, age: Int, grade: Int)