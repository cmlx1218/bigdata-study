package com.cmlx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @Author cmlx
  * @Date 2020-6-23 0023 15:49
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    //1 创建sparkConf并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    //2 创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3 使用sc创建RDD并执行响应的transformation 和 action
    //sc.textFile("spark-study/in").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).saveAsTextFile("out/wordCount");
    //3 读取文件
    val lines: RDD[String] = sc.textFile("spark-study/in")
    //4 将一行一行的数据分解为一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //5 为了方便统计，将单词数据进行结构转换
    val wordToOne: RDD[(String,Int)] = words.map((_,1))
    //6 对转换后的结构进行分组
    val wordToSum: RDD[(String,Int)] = wordToOne.reduceByKey(_+_)
    val sum = wordToSum.sortBy(_._2,false)
    //7 将统计结果采集后打印到控制台
    val result: Array[(String,Int)] = sum.collect()
    result.foreach(println)

    //4 关闭连接
    sc.stop();

  }


}
