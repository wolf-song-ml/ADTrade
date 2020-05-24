package com.z.biz.analyse.analog

import java.util.Date

import com.z.biz.analyse.analog.SessionStatics.{filterComplex, findUserVisitAction, sessionJoinUser}
import com.z.biz.common.conf.{ConfigurationManager, Constants}
import com.z.biz.common.model.UserVisitAction
import org.apache.commons.lang.StringUtils
import com.z.biz.common.utils.{DateUtils, StringUtils => StringUDF}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * TOP K问题
 */
object TopK {
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

    // filter join userVisitAction
    val filteredDetailRDD: RDD[(String, UserVisitAction)] = filteredJoinSession(filteredRDD, actionOriginRDD)
    filteredDetailRDD.persist(StorageLevel.MEMORY_ONLY)

    // top10 Category
    val taskId = DateUtils.dateToString(DateUtils.yyyyMMddHHmmss, new Date())
    val top10CategoryRDD: RDD[Top10Category] = top10Category(filteredDetailRDD, taskId, spark)

    // top 10 session
    top10CategoryTakeSession(top10CategoryRDD, filteredDetailRDD, taskId, spark)
  }

  /**
   * top 10 category下抽取top 10 session
   *
   * @param top10CategoryRDD
   * @param filteredDetailRDD
   * @param taskId
   * @param spark
   */
  def top10CategoryTakeSession(top10CategoryRDD: RDD[Top10Category], filteredDetailRDD: RDD[(String, UserVisitAction)],
                               taskId: String, spark: SparkSession) {
    val categorySessionRDD = filteredDetailRDD.groupByKey()
      .flatMap {
        case (sessionId, userVisitActions) =>
          val sessionCountMap: collection.mutable.HashMap[Long, Int] = new collection.mutable.HashMap()

          for (userVisitAction <- userVisitActions) {
            val clickId: Long = userVisitAction.click_category_id
            if (clickId != null && clickId != -1L) {
              sessionCountMap.get(clickId) match {
                case None => sessionCountMap(clickId) = 0
                case Some(count) => sessionCountMap(clickId) = count + 1
              }
            }
          }

          for ((categoryId, count) <- sessionCountMap)
            yield (categoryId, sessionId + "," + count)
      }

    val categoryJoin = top10CategoryRDD.map(x => (x.categoryid, x.categoryid))

    val topSessionRDD: RDD[Top10Session] = categoryJoin.join(categorySessionRDD)
      .map {
        case (categoryId, (categoryId1, line)) => (categoryId, line)
      }.groupByKey()
      .flatMap {
        case (categoryId, lines) =>
          val linesArray = lines.toArray.sortWith(_.split(",")(1) > _.split(",")(1)).take(10)

          for (item <- linesArray)
            yield Top10Session(taskId, categoryId, item.split(",")(0), item.split(",")(1).toLong)
      }

    import spark.implicits._
    SessionRandomSample.dfSave(topSessionRDD.toDF(), "top10_session")
  }

  /**
   * filter join userVisitAction
   *
   * @param filteredRDD
   * @param actionOriginRDD
   * @return
   */
  def filteredJoinSession(filteredRDD: RDD[(String, String)], actionOriginRDD: RDD[(String, UserVisitAction)]): RDD[(String, UserVisitAction)] = {
    filteredRDD.join(actionOriginRDD)
      .map {
        case (sessionId, (sessionAggrInfo, userVisitAction)) =>
          (sessionId, userVisitAction)
      }
  }

  /**
   * top10 category(click、order、pay综合排序)
   *
   * @param filteredDetailRDD
   * @param taskId
   * @param spark
   * @return
   */
  def top10Category(filteredDetailRDD: RDD[(String, UserVisitAction)], taskId: String, spark: SparkSession): RDD[Top10Category] = {
    // clickCategory
    val clickCategoryRDD: RDD[(Long, Long)] = filteredDetailRDD.filter {
      case (sessionId, userVisitAction) =>
        if (userVisitAction.click_category_id != null && userVisitAction.click_category_id != -1L) true else false
    }.map {
      case (sessionId, userVisitAction) => (userVisitAction.click_category_id, 1L)
    }.reduceByKey(_ + _)

    // orderCategory
    val orderCategoryRDD: RDD[(Long, Long)] = filteredDetailRDD
      .filter {
        case (sessionId, userVisitAction) =>
          val flag = userVisitAction.order_category_ids
          if (StringUtils.isNotBlank(flag) && StringUDF.trimComma(flag).split(",").length > 0) true else false
      }.flatMap {
      case (sessionId, userVisitAction) =>
        StringUDF.trimComma(userVisitAction.order_category_ids).split(",").map(item => (item.toLong, 1L))
    }.reduceByKey(_ + _)

    // payCategory
    val payCategoryRDD: RDD[(Long, Long)] = filteredDetailRDD
      .filter {
        case (sessionId, userVisitAction) =>
          val flag = userVisitAction.pay_category_ids
          if (StringUtils.isNotBlank(flag) && StringUDF.trimComma(flag).split(",").length > 0) true else false
      }.flatMap {
      case (sessionId, userVisitAction: UserVisitAction) =>
        StringUDF.trimComma(userVisitAction.pay_category_ids).split(",").map(item => (item.toLong, 1L))
    }.reduceByKey(_ + _)

    val categoryIdsRDD: RDD[(Long, Long)] = filteredDetailRDD.flatMap { case (sessionId, userVisitAction) =>
      val idsArray = new ArrayBuffer[(Long, Long)]()

      if (userVisitAction.click_category_id != null && userVisitAction.click_category_id != -1L) {
        idsArray += ((userVisitAction.click_category_id, userVisitAction.click_category_id))
      }

      if (StringUtils.isNotBlank(userVisitAction.order_category_ids)) {
        val ids = StringUDF.trimComma(userVisitAction.order_category_ids)
        for (id <- ids.split(",")) {
          idsArray += ((id.toLong, id.toLong))
        }
      }

      if (StringUtils.isNotBlank(userVisitAction.pay_category_ids)) {
        val ids = StringUDF.trimComma(userVisitAction.pay_category_ids)
        for (id <- ids.split(",")) {
          idsArray += ((id.toLong, id.toLong))
        }
      }
      idsArray
    }.distinct()

    val clickJoinRDD: RDD[(Long, String)] = categoryIdsRDD.leftOuterJoin(clickCategoryRDD)
      .map { case (categoryId, (id, ov)) =>
        (categoryId, Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + ov.getOrElse(0))
      }

    val orderJoinRDD: RDD[(Long, String)] = clickJoinRDD.leftOuterJoin(orderCategoryRDD)
      .map { case (categoryId, (line, ov)) =>
        (categoryId, line + "|" + Constants.FIELD_ORDER_COUNT + "=" + ov.getOrElse(0))
      }

    val payJoinRDD: RDD[(Long, String)] = orderJoinRDD.leftOuterJoin(payCategoryRDD)
      .map { case (categoryId, (line, ov)) =>
        (categoryId, line + "|" + Constants.FIELD_PAY_COUNT + "=" + ov.getOrElse(0))
      }

    val sortRDD = payJoinRDD.map { case (categoryId, line) =>
      val clickCount = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT)
      val orderCount = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT)
      val payCount = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT)

      (CategorySortKey(clickCount.toLong, orderCount.toLong, payCount.toLong), line.toString)
    }.sortByKey(false)

    val top10Category = sortRDD.take(10)
      .map { case (categorySortKey, line) =>
        val categoryId = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUDF.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong
        Top10Category(taskId, categoryId, clickCount, orderCount, payCount)
      }

    import spark.implicits._
    val top10CategoryRDD = spark.sparkContext.makeRDD(top10Category)
    SessionRandomSample.dfSave(top10CategoryRDD.toDF(), "top10_category")

    top10CategoryRDD
  }

}
