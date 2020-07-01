package com.cmlx.spark.mysql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @Author cmlx
  * @Date 2020-6-29 0029 10:15
  */
object MysqlWrite {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(List(
      Map("userName" -> "周芷若",
        "age" -> 20, "hobby" -> "九阴白骨爪",
        "address" -> "峨眉")
      , Map("userName" -> "张无忌",
        "age" -> 21, "hobby" -> "九阳神功",
        "address" -> "光明顶")
      , Map("userName" -> "赵敏",
        "age" -> 21, "hobby" -> "调皮",
        "address" -> "京都")
    ))

    data.foreachPartition(insertData)

  }

  def insertData(iterator: Iterator[Map[String, Any]]): Unit = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://39.96.178.201/rdd", "root", "123456")
    iterator.foreach(data =>{
      val ps = conn.prepareStatement("insert into rddtable(userName,age,hobby,address) values(?,?,?,?)")
      ps.setString(1,data.getOrElse("userName","").asInstanceOf[String])
      ps.setInt(2,data.getOrElse("age",0).asInstanceOf[Int])
      ps.setString(3,data.getOrElse("hobby","").asInstanceOf[String])
      ps.setString(4,data.getOrElse("address","").asInstanceOf[String])
      ps.executeUpdate()
    })
  }
}
