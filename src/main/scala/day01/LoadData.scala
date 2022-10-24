package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LoadData {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("LoadData")
    //创建程序入口对象
    val sc = new SparkContext(conf)
    //从本地文件系统加载数据
    val rdd: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\sanguo.txt")
    //从HDFS文件系统加载数据
    val rdd2: RDD[String] = sc.textFile("hdfs://linux01:8020/test1/test1.txt")
    //从本地文件系统加载Sequence文件,将数据反序列化进行读取,KV类型,需要执行classOf[T]
    val rdd3: RDD[(Int, String)] = sc.sequenceFile("d:\\seq", classOf[Int], classOf[String])
    //从集合加载数据
    val rdd4: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6))
    //打印
    rdd.foreach(println)
    rdd2.foreach(println)
    rdd3.foreach(println)
    rdd4.foreach(println)
    //结束
    sc.stop()
  }

}
