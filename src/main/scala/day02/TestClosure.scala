package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
    算子函数中用到了外部定义的变量，称为闭包。这个变量需要被序列化，拷贝发送到每一个task中去，如果
    在task中发生修改，并不会对外部变量产生影响。如果外部变量是一个单例对象，那么这个单例对象每个进程
    拷贝一份，线程使用的时候会去进程中找，如果某个线程修改了变量，那么会产生线程安全问题，但是我们一般
    不会对该变量进行修改。
 */
object TestClosure {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TestAction")
    val sc = new SparkContext(conf)

    //driver
    val mp: mutable.HashMap[Int, String] = new mutable.HashMap[Int,String]()
    mp.put(1,"zhangsan")
    mp.put(2,"lisi")

    val rdd: RDD[(Int, String, Int)] = sc.parallelize(Seq(
      (1, "red", 99),
      (2, "pink", 89),
    ))
    //map
    rdd.map(tp=>{
      val name: String = mp.get(tp._1).get
      (tp._1,tp._2,name)
    }).foreach(println)



    sc.stop()
  }
}
