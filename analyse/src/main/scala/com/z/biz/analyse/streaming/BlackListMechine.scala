package com.z.biz.analyse.streaming

import java.util.Date

import com.z.biz.common.conf.ConfigurationManager
import com.z.biz.common.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ArrayBuffer

/**
 * 黑名单机制
 */
object BlackListMechine {

  private [this] val brokerList = ConfigurationManager.config.getString("kafka.broker.list")

  val topics = ConfigurationManager.config.getString("kafka.topics")

  val threashold = ConfigurationManager.config.getString("blacklist.threashold").toInt

  // kafka消费者配置
  val kafkaParam = Map(
    "bootstrap.servers" -> this.brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "ad-realtime-group",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def main(args: Array[String]): Unit = {
    // spark conf
    val sparkConf = new SparkConf().setAppName("BlackMechine").setMaster("local[1]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint(ConfigurationManager.config.getString("stream.checkpoint"))

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(this.topics), this.kafkaParam))

    val adRealTimeValDstream = adRealTimeLogDStream.map(consumerRecored => consumerRecored.value())

    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
    val adRealTimeDstream = adRealTimeValDstream.repartition(40)

    val filteredDstream = filterBlackList(spark, adRealTimeDstream)

    dynaticBlackList(filteredDstream, this.threashold, spark)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 过滤黑名单： timestamp + " " + province + " " + city + " " + userid + " " + adid
   *
   * @param adRealTimeDstream
   */
  def filterBlackList(spark: SparkSession, adRealTimeDstream: DStream[String]): DStream[(Long, String)] = {
    adRealTimeDstream.transform { logRDD =>
      // blackList
      val blackLists: Array[AdBlacklist] = AdBlacklistDAO.findAll()
      val blackListRDD: RDD[(Long, Boolean)] = spark.sparkContext.makeRDD(blackLists).map(blackList => (blackList.userid, true))

      // log
      val userLogRDD: RDD[(Long, String)] = logRDD.map { log =>
        val logArray = log.split(" ", 5)
        (logArray(3).toLong, log)
      }

      // filter
      val filteredRDD: RDD[(Long, String)] = userLogRDD.leftOuterJoin(blackListRDD)
        .filter { case (userId, (log, flag)) =>
          if (flag.isDefined && flag.get) false else true
        }.map {
          case (userId, (log, flag)) => (userId, log)
        }

      filteredRDD
    }
  }

  /**
   * 动态生成黑名单
   *
   * @param filteredDstream
   */
  def dynaticBlackList(filteredDstream: DStream[(Long, String)], threashold: Int, spark: SparkSession): Unit = {
    // count by key
    val countByKeyDstream: DStream[(String, Long)] = filteredDstream.map { case (userId, log) =>
      val logArray = log.split(" ", 5)
      val date = DateUtils.dateToString(DateUtils.YYYYMMDD, new Date(logArray(0).toLong))
      (date + "_" + userId + "_" + logArray(4), 1L)
    }.reduceByKey(_ + _)

    // clickCount 保存到mysql
    countByKeyDstream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val clickCounts = new ArrayBuffer[AdUserClickCount]()

        for (item <- items) {
          val logArray = item._1.split("_", 3)
          clickCounts += AdUserClickCount(logArray(0), logArray(1).toLong, logArray(2).toLong, item._2)
        }

        AdUserClickCountDAO.updateBatch(clickCounts.toArray)
      }
    }

    // 生成blackList
    val blackListDstream = countByKeyDstream.filter { item =>
      val logArray = item._1.split("_", 3)
      val stock = AdUserClickCountDAO.findClickCountByMultiKey(logArray(0), logArray(1).toLong, logArray(2).toLong)
      if (stock >= threashold) true else false
    }

    val blackUserDstream = blackListDstream.map(x => x._1.split("_", 3)(1).toLong)
    val distinctDstream = blackUserDstream.transform(rdd => rdd.distinct())
//    distinctDstream.print()

    // 保存blackList
    distinctDstream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val blackArray = new ArrayBuffer[AdBlacklist]()

        for (item <- items) {
          blackArray += AdBlacklist(item)
        }

        AdBlacklistDAO.insertBatch(blackArray.toArray)
      }
    }
  }


}
