package day01

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestMapPartitoons {
  def main(args: Array[String]): Unit = {
    //配置文件
    val conf: SparkConf = new SparkConf()
    conf.setAppName("TestMapPartitoons")
    conf.setMaster("local")
    //SparkContext
    val sc: SparkContext = new SparkContext(conf)
    //读取数据源
    val line: RDD[String] = sc.textFile("D:\\Code\\Spark_DY\\datas\\test.txt")
    //map
    val result: RDD[String] = line.map(line => {
      val arr: Array[String] = line.split(",")
      val id: Int = arr(0).toInt
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimezone=UTC", "root", "123456")
      val stmt: PreparedStatement = conn.prepareStatement("select name,age from user where id = ?")
      stmt.setInt(1, id)
      val rs: ResultSet = stmt.executeQuery()
      rs.next()
      val name: String = rs.getString(1)
      val age: Int = rs.getInt(2)
      line + "," + name + "," + age
    })
    //foreach
    result.foreach(println)
    //stop
    sc.stop()
  }
}
