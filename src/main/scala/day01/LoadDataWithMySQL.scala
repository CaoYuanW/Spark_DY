package day01

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从MySQL读取数据
 */
object LoadDataWithMySQL {
  def main(args: Array[String]): Unit = {
    //创建配置环境对象
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("LoadDataWithMySQL")
    //创建程序入口对象
    val sc = new SparkContext(conf)
    //连接对象
    val getConnection = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimezone=UTC", "root", "123456")
    //SQL对象
    val sql = "select * from user where id >= ? and id<= ?"
    //mapRow
    val mapRow = (rs: ResultSet) => {
      val id: Int = rs.getInt(1)
      val name: String = rs.getString(2)
      val age: Int = rs.getInt(3)
      User(id, name, age)
    }
    //从MySQL加载数据源
    val rdd: JdbcRDD[User] = new JdbcRDD[User](sc, getConnection, sql, 1, 10, 1, mapRow)
    //遍历读取
    rdd.foreach(println)
    //停止
    sc.stop()
  }
}


//User类
case class User(id: Int, name: String, age: Int)
