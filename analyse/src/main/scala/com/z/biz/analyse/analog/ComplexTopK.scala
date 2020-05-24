package com.z.biz.analyse.analog

import com.alibaba.fastjson.JSON
import com.z.biz.common.conf.{ConfigurationManager, Constants}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * RDD TOP K 不适宜此应用场景
 */
object ComplexTopK {
  def main(args: Array[String]): Unit = {
    // spark conf
    val sparkConf = new SparkConf().setAppName("SessionStatics").setMaster("local[*]")

    // init spark client
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    spark.udf.register("get_json_object", (json: String, field: String) => {
      val jsonObject = JSON.parseObject(json)
      jsonObject.getString(field)
    })

    spark.udf.register("concat_distinct", new ConcatDistinct())

    val params = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    top3ByArea(spark)

  }

  /**
   * 区域内排行榜top 3：hive搞定
   *
   * @param spark
   */
  def top3ByArea(spark: SparkSession) {
    val sql1 = "SELECT city_id, click_product_id " +
      "FROM user_visit_action " +
      "WHERE click_product_id IS NOT NULL and click_product_id != -1L "
    val clickActionDF = spark.sql(sql1)

    val cities = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"),
      (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))

    import spark.implicits._
    val cityInfoDF = spark.sparkContext.makeRDD(cities)
      .toDF("city_id", "city_name", "area")

    val clickCityDF = clickActionDF.join(cityInfoDF, "city_id")
      .createOrReplaceTempView("tmp_click_product_basic")

/*    val sql2 = "SELECT area, click_product_id, click_count,city_info, row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank," +
      "pi.product_name, if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      " FROM (" +
          " SELECT area, click_product_id, count(*) click_count, concat_ws(',', collect_set(city_info)) city_info FROM (" +
              " SELECT area, click_product_id, concat(city_id,':',city_name) city_info FROM tmp_click_product_basic " +
          ") t GROUP BY area,click_product_id" +
      ") a JOIN product_info pi ON a.click_product_id=pi.product_id "*/

    // 自定义函数去重
        val sql2 = "SELECT area, click_product_id, click_count,city_info, row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank," +
      "pi.product_name, if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      " FROM (" +
          " SELECT area, click_product_id, count(*) click_count, concat_distinct(city_info) city_info FROM (" +
              " SELECT area, click_product_id, concat(city_id,':',city_name) city_info FROM tmp_click_product_basic " +
          ") t GROUP BY area,click_product_id" +
      ") a JOIN product_info pi ON a.click_product_id=pi.product_id "

    val df = spark.sql(sql2).show(false)
  }


}
