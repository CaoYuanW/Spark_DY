package day02

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器的意义在于可以求得程序运行过程中的算子执行时间，失败任务条数，任务处理总条数等信息
 * task分布在不同的进程中执行，先算每个task负责的分区有多少条不合法数据，在汇总不同task的结果不合法数据
 */
object TestAccumulator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("TestAccumulator")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val jsonRDD: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\json.txt")

    //定义累加器
    val acc: LongAccumulator = sc.longAccumulator("MyAccumulator")


    jsonRDD.map(line => {
      try {
        val jSONObject: JSONObject = JSON.parseObject(line)
        val id: Integer = jSONObject.getInteger("id")
        val age: Integer = jSONObject.getInteger("age")
        val grade: Integer = jSONObject.getInteger("grade")
        (id, age, grade)
      } catch {
        case e: Exception => ()
          acc.add(1L)
      }
    }).foreach(println)

    //获取累加器的值
    println(acc.value)

    sc.stop()


  }
}
