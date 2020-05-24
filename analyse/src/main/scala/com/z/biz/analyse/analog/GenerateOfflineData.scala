package com.z.biz.analyse.analog

import java.util.{Date, UUID}

import com.z.biz.common.model.{ProductInfo, UserInfo, UserVisitAction}
import com.z.biz.common.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GenerateOfflineData {
  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  val vocation = Array("医生", "教师", "司机", "工程师", "行政文秘", "企业管理", "销售", "策划", "记者", "会计", "主持人", "编辑",
    "公务员", "军人", "编导", "个体工商", "警察")
  val names = Array("王", "李", "张", "刘", "陈", "杨", "黄", "赵", "吴", "周", "徐", "孙", "马", "朱", "胡", "郭", "何", "高",
    "林", "罗", "郑", "梁", "谢", "宋", "唐", "许", "韩", "冯", "邓", "曹", "彭", "曾", "萧", "田", "董", "潘", "袁", "于",
    "蒋", "蔡", "余", "杜", "叶", "程", "苏", "魏", "吕", "丁", "任", "沈", "姚", "卢", "姜", "崔", "钟", "谭", "陆", "汪", "范",
    "金", "石", "廖", "贾", "夏", "韦", "傅", "方", "白", "邹", "孟", "熊", "秦", "邱", "江", "尹")
  val ascii = Array( "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
    "u", "v", "w", "x", "y", "z")
  val cities = Array("上海", "北京", "深圳", "广州", "重庆", "天津", "苏州", "成都", "武汉", "杭州", "南京", "青岛",
    "无锡", "长沙", "宁波", "郑州", "佛山", "泉州", "南通", "西安")

  val products = Array("荣耀10青春版", "TCL4K电视", "小米（MI）电视", "小米Play", "沁州黄小米", "荣耀10GT游戏加速", "小米路由器4",
    "瑞士watch", "HLA牛仔裤", "AMANI手表", "GKK男鞋", "五福鼠手链", "休闲T恤", "掌阅smartx", "雅培1段", "三元欧酸奶", "iphone x")

  /**
   * userVisitAction
   * @return
   */
  private def generateUserVisitAction(): Array[UserVisitAction] = {
    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")

    // yyyy-MM-dd
    val date : String = DateUtils.dateToString("yyyy-MM-dd", new Date())

    // 关注四个行为：搜索、点击、下单、支付
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    // 一共100个用户（有重复）
    for (i <- 0 to 100) {
      val userid = random.nextInt(100)

      // 每个用户产生10个session
      for (j <- 0 to 10) {
        // 不可变的，全局的，独一无二的128bit长度的标识符，用于标识一个session，体现一次会话产生的sessionId是独一无二的
        val sessionid = DateUtils.dateToString(DateUtils.yyyyMMddHHmmss, new Date()) +
          UUID.randomUUID().toString().replace("-", "").substring(0, 2)

        // 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
        val baseActionTime = date + " " + random.nextInt(23)

        // 每个(userid + sessionid)生成0-100条用户访问数据
        for (k <- 0 to random.nextInt(100)) {
          val pageid = random.nextInt(10)

          // 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) +
            ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))

          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong

          // 随机确定用户在当前session中的行为
          val action = actions(random.nextInt(4))

          // 根据随机产生的用户行为action决定对应字段的值
          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => clickCategoryId = random.nextInt(100).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid, pageid, actionTime, searchKeyword, clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityid)
        }
      }
    }

    rows.toArray
  }

  /**
   * userInfo
   * name、professional、city、sex中文
   * @return
   */
  private def generateUserInfo(): Array[UserInfo] = {
    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("男", "女")
    val random = new Random()
    val ageRandom = new Random(14)

    // 随机产生100个用户的个人信息
    for (i <- 0 to 100) {
      val userid = i
      val username = Array.fill(4)(ascii(random.nextInt(ascii.size))).mkString("")
      val name = Array.fill(3)(names(random.nextInt(names.length))).mkString("")
      val age = ageRandom.nextInt(60)

      val professional = vocation(random.nextInt(vocation.length-1))
      val city = cities(random.nextInt(cities.size))
      val sex = sexes(random.nextInt(2))

      rows += UserInfo(userid, username, name, age, professional, city, sex)
    }

    rows.toArray
  }

  /**
   * productInfo
   * @return
   */
  private def generateProductInfo(): Array[ProductInfo] = {
    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    // 随机产生100个产品信息
    for (i <- 0 to 100) {
      val productId = i
      val productName = products(random.nextInt(products.size))
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }

  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val sparkConf = new SparkConf().setAppName("GenerateOfflineData").setMaster("local[*]")

    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 将模拟数据装换为RDD
    val userVisitActionRdd = spark.sparkContext.makeRDD(generateUserVisitAction())
    val userInfoRdd = spark.sparkContext.makeRDD(generateUserInfo())
    val productInfoRdd = spark.sparkContext.makeRDD(generateProductInfo())

    import spark.implicits._

    userInfoRdd
      .toDF()
      .write
      .mode("overwrite")
      .saveAsTable(USER_INFO_TABLE)

    productInfoRdd
      .toDF()
      .write
      .mode("overwrite")
      .saveAsTable(PRODUCT_INFO_TABLE)

    userVisitActionRdd
      .toDF()
      .write
      .mode("overwrite")
      .saveAsTable(USER_VISIT_ACTION_TABLE)

    spark.stop()

  }

}
