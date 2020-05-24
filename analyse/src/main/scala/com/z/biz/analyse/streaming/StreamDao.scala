package com.z.biz.analyse.streaming

import java.sql.ResultSet
import com.z.biz.common.utils.{MysqlPoolUtils, QueryCallback}
import scala.collection.mutable.ArrayBuffer

/**
 * 用户黑名单DAO类
 */
object AdBlacklistDAO {

  /**
   * 批量插入广告黑名单用户
   *
   * @param adBlacklists
   */
  def insertBatch(adBlacklists: Array[AdBlacklist]) {
    // 批量插入
    val sql = "INSERT INTO ad_blacklist VALUES(?)"

    val paramsList = new ArrayBuffer[Array[Any]]()

    // 向paramsList添加userId
    for (adBlacklist <- adBlacklists) {
      val params: Array[Any] = Array(adBlacklist.userid)
      paramsList += params
    }

    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 执行批量插入操作
    client.executeBatch(sql, paramsList.toArray)
    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

  /**
   * 查询所有广告黑名单用户
   *
   * @return
   */
  def findAll(): Array[AdBlacklist] = {
    // 将黑名单中的所有数据查询出来
    val sql = "SELECT * FROM ad_blacklist"

    val adBlacklists = new ArrayBuffer[AdBlacklist]()

    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 执行sql查询并且通过处理函数将所有的userid加入array中
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          val userid = rs.getInt(1).toLong
          adBlacklists += AdBlacklist(userid)
        }
      }
    })

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
    adBlacklists.toArray
  }
}


/**
 * 用户广告点击量DAO实现类
 */
object AdUserClickCountDAO {
  /**
   * 批量插入、更新
   *
   * @param adUserClickCounts ：Array[AdUserClickCount]
   */
  def updateBatch(adUserClickCounts: Array[AdUserClickCount]) {
    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 首先对用户广告点击量进行分类，分成待插入的和待更新的
    val insertAdUserClickCounts, updateAdUserClickCounts = ArrayBuffer[AdUserClickCount]()

    val selectSQL = "SELECT count(*) FROM ad_user_click_count WHERE date=? AND userid=? AND adid=? "
    for (adUserClickCount <- adUserClickCounts) {
      val selectParams: Array[Any] = Array(adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
      client.executeQuery(selectSQL, selectParams, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          if (rs.next() && rs.getInt(1) > 0) {
            updateAdUserClickCounts += adUserClickCount
          } else {
            insertAdUserClickCounts += adUserClickCount
          }
        }
      })
    }

    // 执行批量插入
    val insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adUserClickCount <- insertAdUserClickCounts) {
      insertParamsList += Array[Any](adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid, adUserClickCount.clickCount)
    }
    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 执行批量更新
    val updateSQL = "UPDATE ad_user_click_count SET clickCount=clickCount + ? WHERE date=? AND userid=? AND adid=?"
    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adUserClickCount <- updateAdUserClickCounts) {
      updateParamsList += Array[Any](adUserClickCount.clickCount, adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
    }
    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

  /**
   * 根据多个key查询用户广告点击量
   *
   * @param date   日期
   * @param userid 用户id
   * @param adid   广告id
   * @return
   */
  def findClickCountByMultiKey(date: String, userid: Long, adid: Long): Int = {
    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()

    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    val sql = "SELECT clickCount FROM ad_user_click_count WHERE date=? AND userid=? AND adid=?"
    var clickCount = 0
    val params = Array[Any](date, userid, adid)

    // 根据多个条件查询指定用户的点击量，将查询结果累加到clickCount中
    client.executeQuery(sql, params, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        if (rs.next()) {
          clickCount = rs.getInt(1)
        }
      }
    })

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
    clickCount
  }
}


/**
 * 广告实时统计DAO实现类
 */
object AdStatDAO {

  /**
   * 批量插入、更新标
   *
   * @param adStats
   */
  def updateBatch(adStats: Array[AdStat]) {
    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()

    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 区分开来哪些是要插入的，哪些是要更新的
    val insertAdStats, updateAdStats = ArrayBuffer[AdStat]()
    val selectSQL = "SELECT count(*) FROM ad_stat WHERE date=? AND province=? AND city=? AND adid=?"

    for (adStat <- adStats) {
      val params = Array[Any](adStat.date, adStat.province, adStat.city, adStat.adid)
      client.executeQuery(selectSQL, params, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          if (rs.next() && rs.getInt(1) > 0) {
            updateAdStats += adStat
          } else {
            insertAdStats += adStat
          }
        }
      })
    }

    // 批量插入
    val insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adStat <- insertAdStats) {
      insertParamsList += Array[Any](adStat.date, adStat.province, adStat.city, adStat.adid, adStat.clickCount)
    }
    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 批量更新
    val updateSQL = "UPDATE ad_stat SET clickCount=? WHERE date=? AND province=? AND city=? AND adid=?"
    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adStat <- updateAdStats) {
      updateParamsList += Array[Any](adStat.clickCount, adStat.date, adStat.province, adStat.city, adStat.adid)
    }
    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }
}


/**
 * 各省份top3热门广告DAO实现类
 *
 */
object AdProvinceTop3DAO {

  def updateBatch(adProvinceTop3s: Array[AdProvinceTop3]) {
    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()

    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    val dateProvinces = ArrayBuffer[String]()
    for (adProvinceTop3 <- adProvinceTop3s) {
      val key = adProvinceTop3.date + "_" + adProvinceTop3.province
      // 借此去重
      if (!dateProvinces.contains(key)) {
        dateProvinces += key
      }
    }

    // 根据去重后的date和province，进行批量删除操作
    val deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?"
    val deleteParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (dateProvince <- dateProvinces) {
      val dateProvinceSplited = dateProvince.split("_")
      val date = dateProvinceSplited(0)
      val province = dateProvinceSplited(1)
      val params = Array[Any](date, province)
      deleteParamsList += params
    }
    client.executeBatch(deleteSQL, deleteParamsList.toArray)

    // 批量插入
    val insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adProvinceTop3 <- adProvinceTop3s) {
      insertParamsList += Array[Any](adProvinceTop3.date, adProvinceTop3.province, adProvinceTop3.adid, adProvinceTop3.clickCount)
    }
    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }
}


/**
 * 广告点击趋势DAO实现类
 *
 */
object AdClickTrendDAO {

  def updateBatch(adClickTrends: Array[AdClickTrend]) {
    // 获取对象池单例对象
    val mySqlPool = MysqlPoolUtils()

    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 区分开来哪些是要插入的，哪些是要更新的
    val updateAdClickTrends, insertAdClickTrends = ArrayBuffer[AdClickTrend]()

    val selectSQL = "SELECT count(*) FROM ad_click_trend WHERE date=? AND hour=? AND minute=? AND adid=?"
    for (adClickTrend <- adClickTrends) {
      val params = Array[Any](adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid)
      client.executeQuery(selectSQL, params, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          if (rs.next() && rs.getInt(1) > 0) {
            updateAdClickTrends += adClickTrend
          } else {
            insertAdClickTrends += adClickTrend
          }
        }
      })
    }

    // 执行批量更新操作
    val updateSQL = "UPDATE ad_click_trend SET clickCount=? WHERE date=? AND hour=? AND minute=? AND adid=?"
    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adClickTrend <- updateAdClickTrends) {
      updateParamsList += Array[Any](adClickTrend.clickCount, adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid)
    }
    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 批量插入
    val insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
    for (adClickTrend <- insertAdClickTrends) {
      insertParamsList += Array[Any](adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid, adClickTrend.clickCount)
    }
    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }
}
