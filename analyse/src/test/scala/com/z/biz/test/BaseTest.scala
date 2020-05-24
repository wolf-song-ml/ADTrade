package com.z.biz.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BaseTest {
  def main1(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SessionStatics").setMaster("local[1]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val testRdd = spark.sparkContext.makeRDD(Seq((1, "one"), (1, "two"), (3, "three"), (3, "four")))

    val re = testRdd.groupByKey().flatMap{
      case (key, items)=>
        items.toList
    }.collect()

    re.foreach(println(_))

    val rem = testRdd.groupByKey().map{
      case (key, items)=>
        items.toList
    }.collect()

    rem.foreach(println(_))

  }

  def main(args: Array[String]): Unit = {
    val cities = Array("上海", "北京", "深圳", "广州", "重庆", "天津", "苏州", "成都", "武汉", "杭州", "南京", "青岛",
      "无锡", "长沙", "宁波", "郑州", "佛山", "泉州", "南通", "西安")
    println(cities(0))
  }
}
