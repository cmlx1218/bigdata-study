package com.cmlx.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @Author cmlx
  * @Date 2020-6-29 0029 14:55
  */
object HBaseSpark {

  def main(args: Array[String]): Unit = {

    //1 创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2 创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3 构建HBase配置信息
    val conf:Configuration = HBaseConfiguration.create()


  }

}
