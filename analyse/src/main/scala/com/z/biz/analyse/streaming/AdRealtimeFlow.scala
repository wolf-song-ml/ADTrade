package com.z.biz.analyse.streaming

import java.util.Date

import com.z.biz.analyse.streaming.BlackListMechine.{dynaticBlackList, filterBlackList}
import com.z.biz.common.conf.ConfigurationManager
import com.z.biz.common.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * 广告实时流量统计
 */
object AdRealtimeFlow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // spark conf
    val sparkConf = new SparkConf().setAppName("BlackMechine").setMaster("local[1]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint(ConfigurationManager.config.getString("stream.checkpoint"))
    //    spark.sparkContext.setLogLevel("WARN")

    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(BlackListMechine.topics), BlackListMechine.kafkaParam))

    // repartition
    val adRealTimeValDstream = adRealTimeLogDStream.map(consumerRecored => consumerRecored.value())
    val adRealTimeDstream = adRealTimeValDstream.repartition(40)

    // black filter and dynatic generate
    val filteredDstream = filterBlackList(spark, adRealTimeDstream)
    dynaticBlackList(filteredDstream, BlackListMechine.threashold, spark)

    // 实时流量统计
    val realTimeDstream = staticsRealtimeFlow(filteredDstream)

    top3ByArea(spark, realTimeDstream)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 统计广告点击实时流量
   * timestamp + " " + province + " " + city + " " + userid + " " + adid
   */
  def staticsRealtimeFlow(filteredDstream: DStream[(Long, String)]): DStream[(String, Long)] = {
    val keyDstream: DStream[(String, Long)] = filteredDstream.map { item =>
      val logArray = item._2.split(" ", 5)
      val date = DateUtils.dateToString(DateUtils.YYYYMMDD, new Date(logArray(0).toLong))
      (date + "_" + logArray(1) + "_" + logArray(2) + "_" + logArray(4), 1L)
    }

    val realTimeDstream: DStream[(String, Long)] = keyDstream.updateStateByKey[Long] {
      (values: Seq[Long], old: Option[Long]) =>
        var clickCount = 0L
        if (old.isDefined) {
          clickCount = old.get
        }

        for (value <- values) {
          clickCount += value
        }

        Some(clickCount)
    }

    realTimeDstream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val adStatBuffer = new ArrayBuffer[AdStat]()
        for (item <- items) {
          val logArray = item._1.split("_", 4)
          adStatBuffer += AdStat(logArray(0), logArray(1), logArray(2), logArray(3).toLong, item._2)
        }

        AdStatDAO.updateBatch(adStatBuffer.toArray)
      }
    }

    realTimeDstream
  }

  /**
   * 实时流量-区域内流量top3: date_province_city_adid
   */
  def top3ByArea(spark: SparkSession, updateKeyDstream: DStream[(String, Long)]): Unit = {
    val sqlDstream: DStream[(String, String, Long, Long)] = updateKeyDstream.transform { rdd =>
      val dateProvinceRDD: RDD[(String, Long)] = rdd.map { item =>
        val logArray = item._1.split("_", 4)
        (logArray(0) + "_" + logArray(1) + "_" + logArray(3).toLong, item._2)
      }

      import spark.implicits._
      val reduceRDD = dateProvinceRDD.reduceByKey(_ + _)

      val foramtRDD: RDD[(String, String, Long, Long)] = reduceRDD.map { item =>
        val logArray = item._1.split("_", 3)
        (logArray(0), logArray(1), logArray(2).toLong, item._2)
      }

      /** 第一种：spark sql实现
      val reduceDF = foramtRDD.toDF("date", "province", "adid", "count")
        .createOrReplaceTempView("tmp_date_province_click")

      val sql = "SELECT date, province, adid, count, ROW_NUMBER() OVER(PARTITION BY province ORDER BY count DESC) rank " +
        "FROM tmp_date_province_click HAVING rank <= 3"

      spark.sql(sql).rdd
      */

      /**
       * 第二种：RDD实现
       */
      val reFormatedRDD = foramtRDD.map { case (date, province, adid, count) =>
        (province, (date, adid, count))
      }

      val endRDD = reFormatedRDD.groupByKey()
        .flatMap { case (province, items) =>
          val retArray = new ArrayBuffer[(String, String, Long, Long)]()
          val itemSeq = items.toSeq.sortWith(_._3 > _._3).take(3)
          for (item <- itemSeq) {
            retArray += ((item._1, province, item._2, item._3))
          }
          retArray
        }

      endRDD
    }

    /**
     * 第一种：spark sql实现
    sqlDstream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val adArray = new ArrayBuffer[AdProvinceTop3]()
        for (item <- items) {
          adArray += AdProvinceTop3(item.getString(0), item.getString(1), item.getLong(2), item.getLong(3))
        }

        AdProvinceTop3DAO.updateBatch(adArray.toArray)
      }
    }
     */

    /**
     * 第二种：RDD实现
     */
    sqlDstream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val adArray = new ArrayBuffer[AdProvinceTop3]()
        for (item <- items) {
          adArray += AdProvinceTop3(item._1, item._2, item._3, item._4)
        }

        AdProvinceTop3DAO.updateBatch(adArray.toArray)
      }
    }

  }


}
