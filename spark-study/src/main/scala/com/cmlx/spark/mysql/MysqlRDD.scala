package com.cmlx.spark.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @Author cmlx
  * @Date 2020-6-28 0028 18:39
  */
object MysqlRDD {

  def main(args: Array[String]): Unit = {

    //1 创建 spark 配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2 创建 SparkContext
    val sc = new SparkContext(sparkConf)

    //3 定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://39.96.178.201:3306/rdd"
    val username = "root"
    val password = "123456"

    // 创建 JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, username, password)
    },
      "select * from `rddtable` where `id`>=? and `id` < ?;",
      1,
      10,
      1,
      r => (r.getInt(1),r.getString(2),r.getInt(3),r.getString(4),r.getString(5))
    )
    // 打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()
  }
}
