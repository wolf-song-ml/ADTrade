package com.z.biz.analyse.analog

import java.util.Date

import com.z.biz.analyse.analog.SessionStatics.{filterComplex, findUserVisitAction, sessionJoinUser}
import com.z.biz.common.conf.{ConfigurationManager, Constants}
import com.z.biz.common.model.UserVisitAction
import com.z.biz.common.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

/**
 * session 随机抽样算法:https://www.jianshu.com/p/4d14dd9c20b2
 */
object SessionRandomSample {
//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val loggerSV = LoggerFactory.getLogger("SVLogger")
  val logger = LoggerFactory.getLogger(SessionRandomSample.getClass)

  def main(args: Array[String]): Unit = {
    // spark conf
    val sparkConf = new SparkConf().setAppName("SessionStatics").setMaster("local[*]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val params = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    // find userVisitAction
    val actionOriginRDD: RDD[(String, UserVisitAction)] = findUserVisitAction(spark, params)
    actionOriginRDD.persist(StorageLevel.MEMORY_ONLY)

    // session join user
    val sessionJoinUserRDD: RDD[(String, String)] = sessionJoinUser(spark, actionOriginRDD)

    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    spark.sparkContext.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")

    // search in memory
    val filteredRDD: RDD[(String, String)] = filterComplex(sessionJoinUserRDD, params, sessionAggrStatAccumulator)
    filteredRDD.persist(StorageLevel.MEMORY_ONLY)

    // random sample index
    val sampleDateIndex: mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]] = sessionRandomSampleIndex(filteredRDD, 200)
    val broadcastIndex = spark.sparkContext.broadcast(sampleDateIndex)

    val taskId = DateUtils.dateToString(DateUtils.yyyyMMddHHmmss, new Date())
    randomSampleSession(spark, actionOriginRDD, filteredRDD, taskId, broadcastIndex.value)

  }

  /**
   * random sample session detail
   *
   * @param actionOriginRDD
   * @param filteredRDD
   * @param broadcastIndex
   */
  def randomSampleSession(spark: SparkSession, actionOriginRDD: RDD[(String, UserVisitAction)], filteredRDD: RDD[(String, String)], taskId: String,
                          broadcastIndex: mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]): Unit = {
    // (hour -> count)
    val hourMapRDD = filteredRDD.map {
      case (sessionId, aggrInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", "startTime")
        // yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, aggrInfo)
    }

    val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()

    // session汇总
    val extractRDD: RDD[SessionRandomExtract] = hourMapRDD.groupByKey()
      .flatMap {
        case (dateHour, items) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)

          val indexs: ListBuffer[Int] = broadcastIndex(date)(hour)
          var index = 0

          for (sessionAggrInfo <- items) {
            if (indexs.contains(index)) {
              val sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
              sessionRandomExtractArray += SessionRandomExtract(taskId, sessionId, startTime, searchKeywords, clickCategoryIds)
            }

            index += 1
          }
          sessionRandomExtractArray
      }

    import spark.implicits._
    dfSave(extractRDD.toDF(), "session_random_extract")

    // session 详细
    val extractIdsRDD = extractRDD.map(x => (x.sessionid, x))
    val extractDetailRDD: RDD[SessionDetail] = extractIdsRDD.join(actionOriginRDD)
      .map {
        case (sessionId, (sessionRandomExtract, userVisitAction: UserVisitAction)) =>
          SessionDetail(taskId, userVisitAction.user_id, userVisitAction.session_id, userVisitAction.page_id, userVisitAction.action_time,
            userVisitAction.search_keyword, userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
            userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
      }

    dfSave(extractDetailRDD.toDF(), "session_detail")
  }

  /**
   * save ddataFrame to mysql
   *
   * @param df
   * @param table
   */
  def dfSave(df: DataFrame, table: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", table)
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
   * 随机compute index list buffer
   *
   * @param filteredRDD
   * @param totalCount
   */
  def sessionRandomSampleIndex(filteredRDD: RDD[(String, String)], totalCount: Int):
  mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]] = {
    // (date, (hour->count))
    val hourMapRDD = filteredRDD.map {
      case (sessionId, aggrInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", "startTime")
        // yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, aggrInfo)
    }

    val hourCountMap = hourMapRDD.countByKey
    val dateCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)

      dateCountMap.get(date) match {
        case None => dateCountMap(date) = new mutable.HashMap[String, Long]()
          dateCountMap(date) += (hour -> count) // dateCountMap(date).put(hour, count)
        case Some(hourMap) => hourMap += (hour -> count)
      }
    }

    // (date, (hour, [List]))
    val countPerDay: Int = totalCount / dateCountMap.size
    val sampleDateIndex = new mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()

    for ((date, hourMap) <- dateCountMap) {
      sampleDateIndex.get(date) match {
        case None => sampleDateIndex(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]()
          randomGenerateIndex(sampleDateIndex(date), hourMap, countPerDay)
      }
    }

    sampleDateIndex
  }

  /**
   * 随机生成索引
   *
   * @param sampleHourMap
   * @param hourMap
   * @param countPerDay
   */
  def randomGenerateIndex(sampleHourMap: mutable.HashMap[String, mutable.ListBuffer[Int]], hourMap: mutable.HashMap[String, Long],
                          countPerDay: Int) {
    val random = new Random()
    for ((hour, count) <- hourMap) {
      var countByHour: Int = (countPerDay * (count / hourMap.values.sum.toDouble)).toInt
      if (countByHour > count) {
        countByHour = count.toInt
      }

      sampleHourMap.get(hour) match {
        case None => sampleHourMap(hour) = new mutable.ListBuffer[Int]()
          for (i <- 0 until countByHour) {
            var index = random.nextInt(count.toInt)

            while (sampleHourMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            sampleHourMap(hour) += index
          }
      }
    }

    val sortResult: Seq[(String, ListBuffer[Int])] = sampleHourMap.toSeq.sortWith(_._1.toInt < _._1.toInt)
    val count = sortResult.map {
      case (hour, list) =>
        list.size
    }.reduce(_ + _)

    // sort
    logger.error("排序：count:{}, seq{}", count, sortResult)
    loggerSV.error("SVLogger-排序map：{}", mutable.HashMap(sortResult: _*))
  }


}
