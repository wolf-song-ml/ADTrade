package com.z.biz.analyse.streaming

import java.util.Date

import com.z.biz.common.conf.ConfigurationManager
import com.z.biz.common.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdRealTimeTrends {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // spark conf
    val sparkConf = new SparkConf().setAppName("BlackMechine").setMaster("local[1]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint(ConfigurationManager.config.getString("stream.checkpoint"))

    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(BlackListMechine.topics), BlackListMechine.kafkaParam))

    // repartition
    val adRealTimeValDstream = adRealTimeLogDStream.map(consumerRecored => consumerRecored.value())
    val adRealTimeDstream = adRealTimeValDstream.repartition(40)

    caculateAdTrends(adRealTimeDstream)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 实时流量周期（window）趋势：timestamp + " " + province + " " + city + " " + userid + " " + adid
   *
   * @param adRealTimeDstream
   */
  def caculateAdTrends(adRealTimeDstream: DStream[String]): Unit = {
    val dateKeyDstream = adRealTimeDstream.map { log =>
      val logArray = log.split(" ", 5)
      val dateKey = DateUtils.dateToString("yyyyMMdd_HH_mm", new Date(logArray(0).toLong))
      (dateKey + "_" + logArray(4), 1L)
    }

    val windowDstream: DStream[(String, Long)] = dateKeyDstream.reduceByKeyAndWindow((a: Long, b: Long) => a + b, Minutes(60L), Seconds(10L))

    windowDstream.foreachRDD { rdd =>
      rdd.foreachPartition{items =>
        val buf = new ArrayBuffer[AdClickTrend]()
        for (item <- items ) {
          val logArray = item._1.split("_", 4)
          buf += AdClickTrend(logArray(0), logArray(1), logArray(2), logArray(3).toLong, item._2)
        }
        AdClickTrendDAO.updateBatch(buf.toArray)
      }
    }
  }


}
