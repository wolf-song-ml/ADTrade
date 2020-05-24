package com.z.biz.analyse.analog

import java.util.{Date, UUID}

import com.alibaba.fastjson.JSON
import com.z.biz.common.conf.{ConfigurationManager, Constants}
import com.z.biz.common.model.{UserInfo, UserVisitAction}
import com.z.biz.common.utils.{DateUtils, NumberUtils, StringUtils, ValidUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
 * session 统计分析
 */
object SessionStatics {
  def main(args: Array[String]): Unit = {
    // spark conf
    val sparkConf = new SparkConf().setAppName("SessionStatics").setMaster("local[*]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val params = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    // find userVisitAction
    val actionOriginRDD = findUserVisitAction(spark, params)

    // session join user
    val sessionJoinUserRDD = sessionJoinUser(spark, actionOriginRDD)

    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    spark.sparkContext.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")

    // search in memory
    val filteredRDD = filterComplex(sessionJoinUserRDD, params, sessionAggrStatAccumulator)

    // statics
    filteredRDD.collect()
    val taskId = DateUtils.dateToString(DateUtils.yyyyMMddHHmmss, new Date())
    calculateStatRatio(spark, sessionAggrStatAccumulator.value, taskId)
  }

  /**
   * 统计session ratio
   *
   * @param spark
   * @param value
   * @param taskUUID
   */
  def calculateStatRatio(spark: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String) {
    // get from accumulator
    val sessionCount: Double = value(Constants.SESSION_COUNT).toDouble

    val visit_length_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s: Int = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s: Int = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s: Int = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m: Int = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m: Int = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m: Int = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m: Int = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3: Int = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6: Int = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9: Int = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30: Int = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60: Int = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60: Int = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // visit_length and visit_stepth ratio
    val visit_length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1s_3s / sessionCount, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtils.formatDouble(visit_length_4s_6s / sessionCount, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtils.formatDouble(visit_length_7s_9s / sessionCount, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtils.formatDouble(visit_length_10s_30s / sessionCount, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtils.formatDouble(visit_length_30s_60s / sessionCount, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m / sessionCount, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtils.formatDouble(visit_length_3m_10m / sessionCount, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtils.formatDouble(visit_length_10m_30m / sessionCount, 2)
    val visit_length_30m_ratio: Double = NumberUtils.formatDouble(visit_length_30m / sessionCount, 2)

    val step_length_1_3_ratio: Double = NumberUtils.formatDouble(step_length_1_3 / sessionCount, 2)
    val step_length_4_6_ratio: Double = NumberUtils.formatDouble(step_length_4_6 / sessionCount, 2)
    val step_length_7_9_ratio: Double = NumberUtils.formatDouble(step_length_7_9 / sessionCount, 2)
    val step_length_10_30_ratio: Double = NumberUtils.formatDouble(step_length_10_30 / sessionCount, 2)
    val step_length_30_60_ratio: Double = NumberUtils.formatDouble(step_length_30_60 / sessionCount, 2)
    val step_length_60_ratio: Double = NumberUtils.formatDouble(step_length_60 / sessionCount, 2)

    // 从库里读or写封装成case class：1序列化与反序列化，2shcema数据类型推断
    val sessionAggrStat = SessionAggrStat(taskUUID, sessionCount.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio,
      visit_length_7s_9s_ratio, visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio, visit_length_3m_10m_ratio,
      visit_length_10m_30m_ratio, visit_length_30m_ratio, step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    import spark.implicits._

    val sessionAggrStatRDD = spark.sparkContext.makeRDD(Array(sessionAggrStat))

    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  // 计算访问时长范围
  def calculateStat(visitLen: Long, stepLength: Int, sessionAggrStatAccumulator: SessionAggrStatAccumulator) {
    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

    if (visitLen >= 1 && visitLen <= 3) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLen >= 4 && visitLen <= 6) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLen >= 7 && visitLen <= 9) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLen >= 10 && visitLen <= 30) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLen > 30 && visitLen <= 60) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLen > 60 && visitLen <= 180) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLen > 180 && visitLen <= 600) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLen > 600 && visitLen <= 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLen > 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
    }

    if (stepLength >= 1 && stepLength <= 3) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  /**
   * 搜索过滤
   *
   * @param actionDetailRDD
   * @param searchParams
   */
  def filterComplex(actionDetailRDD: RDD[(String, String)], searchParams: String, sessionAggrStatAccumulator:
  SessionAggrStatAccumulator): RDD[(String, String)] = {
    // all are String
    val jsonObject = JSON.parseObject(searchParams)
    val startAge = jsonObject.getString(Constants.PARAM_START_AGE)
    val endAge = jsonObject.getString(Constants.PARAM_END_AGE)
    val professionals = jsonObject.getString(Constants.PARAM_PROFESSIONALS)
    val cities = jsonObject.getString(Constants.PARAM_CITIES)
    val sex = jsonObject.getString(Constants.PARAM_SEX)
    val keywords = jsonObject.getString(Constants.PARAM_KEYWORDS)
    val categoryIds = jsonObject.getString(Constants.PARAM_CATEGORY_IDS)

    var parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (parameter.endsWith("\\|")) {
      parameter = parameter.substring(0, parameter.length() - 1)
    }

    // 根据筛选参数进行过滤
    val filteredRDD = actionDetailRDD.filter { case (sessionId, aggrInfo) =>
      // 按照年龄范围进行过滤（startAge、endAge）
      var success = true
      if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
        success = false

      // 按照职业范围进行过滤（professionals）
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS))
        success = false

      // 按照城市范围进行过滤（cities）
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES))
        success = false

      // 按照性别进行过滤
      if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX))
        success = false

      // 按照搜索词进行过滤
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS))
        success = false

      // 按照点击品类id进行过滤
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS))
        success = false

      // 累加器统计session时段
      if (success) {
        val visitLen = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)
        val visitDep = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH)

        calculateStat(visitLen.toLong, visitDep.toInt, sessionAggrStatAccumulator)
      }
      success
    }

    filteredRDD
  }

  /**
   * find userVisitAction from hive
   *
   * @param spark
   * @param params
   * @return
   */
  def findUserVisitAction(spark: SparkSession, params: String): RDD[(String, UserVisitAction)] = {
    val jsonObject = JSON.parseObject(params)
    val startDate = jsonObject.get("startDate")
    val endDate = jsonObject.get("endDate")

    import spark.implicits._

    val actionOriginRDD = spark
      .sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
      .as[UserVisitAction]
      .rdd
      .map { userVisitAction =>
        (userVisitAction.session_id, userVisitAction)
      }

    actionOriginRDD
  }

  /**
   * userVisitAction join userInfo
   *
   * @param spark
   * @return
   */
  def sessionJoinUser(spark: SparkSession, actionOriginRDD: RDD[(String, UserVisitAction)]): RDD[(String, String)] = {
    import spark.implicits._

    // userVisitAction group by
    val actionAggRDD = actionOriginRDD
      .groupByKey()
      .map {
        case (sessionId, userVisitActions) =>
          val searchKeywordsBuffer, clickCategoryIdsBuffer = new StringBuffer()
          var visitDep: Int = 0
          var startTime, endTime: Date = null
          var visitLen: Long = 0L
          var userId: Long = -1L

          for (userVisit <- userVisitActions) {
            if (userId == -1L) {
              userId = userVisit.user_id
            }

            if (StringUtils.isNotEmpty(userVisit.search_keyword) &&
              !searchKeywordsBuffer.toString.contains(userVisit.search_keyword)) {
              searchKeywordsBuffer.append(userVisit.search_keyword + ",")
            }

            if (userVisit.click_category_id != -1L &&
              !clickCategoryIdsBuffer.toString.contains(userVisit.click_category_id.toString)) {
              clickCategoryIdsBuffer.append(userVisit.click_category_id + ",")
            }

            // yyyy-MM-dd HH:mm:ss
            val actionTime = DateUtils.stringToDate(DateUtils.yyyyMMddHHmmss_, userVisit.action_time)

            if (startTime == null || actionTime.before(startTime)) {
              startTime = actionTime
            }

            if (endTime == null || actionTime.after(endTime)) {
              endTime = actionTime
            }

            visitDep += 1
          }
          // 单位：h
          visitLen = (endTime.getTime - startTime.getTime) / (1000 * 60 * 60)

          var searchKeywords = searchKeywordsBuffer.toString
          if (searchKeywords.endsWith(",")) {
            searchKeywords = searchKeywords.substring(0, searchKeywords.length() - 1)
          }

          var clickCategoryIds = clickCategoryIdsBuffer.toString
          if (clickCategoryIds.endsWith(",")) {
            clickCategoryIds = clickCategoryIds.substring(0, clickCategoryIds.length() - 1)
          }

          // 聚合数据，使用key=value|key=value
          val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
            Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
            Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
            Constants.FIELD_VISIT_LENGTH + "=" + visitLen + "|" +
            Constants.FIELD_STEP_LENGTH + "=" + visitDep + "|" +
            Constants.FIELD_START_TIME + "=" + DateUtils.dateToString(DateUtils.yyyyMMddHHmmss_, startTime)

          (userId, partAggrInfo)
      }

    // userInfo
    val userOriginRDD = spark
      .sql("select * from user_info")
      .as[UserInfo]
      .rdd
      .map(userInfo => (userInfo.user_id, userInfo))

    // sessionVisitAction join userInfo
    val actionDetailRDD = actionAggRDD.join(userOriginRDD)
      .map {
        case (userId, (partAggrInfo, userInfo)) =>

          val fullAggrInfo = partAggrInfo + "|" +
            Constants.FIELD_AGE + "=" + userInfo.age + "|" +
            Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
            Constants.FIELD_CITY + "=" + userInfo.city + "|" +
            Constants.FIELD_SEX + "=" + userInfo.sex

          val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

          (sessionId, fullAggrInfo)
      }

    actionDetailRDD
  }


}
