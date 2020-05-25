# Spark大数据广告营销分析
## 项目目标
对电商网站的各种用户行为（访问行为、购物行为、广告点击行为等）进行复杂的分析。用统计分析出来的数据，辅助公司中的PM（产品经理）、数据分析师以及管理人员分析现有产品的情况，并根据用户行为分析结果持续改进产品的设计，以及调整公司的战略和业务。最终达到用大数据技术来帮助提升公司的业绩、营业额以及市场占有率的目标。


## 基础准备
### scala mysql连接池
 ```scala
   class PooledMySqlClientFactory(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, client: Option[Connection] = None)
      extends BasePooledObjectFactory[MySqlProxy] with Serializable {
    
      // 用于池来创建对象
      override def create(): MySqlProxy = MySqlProxy(jdbcUrl, jdbcUser, jdbcPassword, client)
    
      // 用于池来包装对象
      override def wrap(obj: MySqlProxy): PooledObject[MySqlProxy] = new DefaultPooledObject(obj)
    
      // 用于池来销毁对象
      override def destroyObject(p: PooledObject[MySqlProxy]): Unit = {
        p.getObject.shutdown()
        super.destroyObject(p)
      }
    
    }
```
### 注入全局配置
  ```scala
// 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过params参数对其初始化
  private val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties().setFileName("commerce.properties"))

  // 通过getConfiguration获取配置对象
  val config = builder.getConfiguration()
```
# 离线业务
## 1用户聚合分布
- 自定义累加器

 ```scala
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    // scala的private是实例可见性而Java的private是类的可见
    aggrStatMap.synchronized {
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }
}
```
- 语法糖
```scala
 (aggrStatMap /: acc.value) {
    case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0)))
 }
```
- foldLeft
```scala
def f (imap : mutable.HashMap[String, Int], tupple :(String, Int)) : mutable.HashMap[String, Int] = {
	tupple match {
		case (k, v)=>
			imap += (k -> (v + imap.getOrElse(k, 0)))
	}
}
acc.value.foldLeft(aggrStatMap)(f)
```


## 2用户随机抽样
这个按照时间比例是什么意思呢？随机抽取本身是很简单的，但是按照时间比例，就很复杂了。
```scala
def sessionRandomSampleIndex(filteredRDD: RDD[(String, String)], totalCount: Int):
  mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]] = {
   ......
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
  .......
    sampleDateIndex
  }
```
## 3 topK 问题
- reduceByKey
```scala
 val clickCategoryRDD: RDD[(Long, Long)] = filteredDetailRDD.filter {
      case (sessionId, userVisitAction) =>
        if (userVisitAction.click_category_id != null && userVisitAction.click_category_id != -1L) true else false
    }.map {
      case (sessionId, userVisitAction) => (userVisitAction.click_category_id, 1L)
    }.reduceByKey(_ + _)
```
- join
```scala
 val clickJoinRDD: RDD[(Long, String)] = categoryIdsRDD.leftOuterJoin(clickCategoryRDD)
   .map { case (categoryId, (id, ov)) =>
     	(categoryId, Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + ov.getOrElse(0))
}
```
- key组合排序
```scala
case class CategorySortKey(clickCount: Long, orderCount: Long, payCount: Long) extends Ordered[CategorySortKey] {

  override def compare(that: CategorySortKey): Int = {
    if ((clickCount - that.clickCount) > 0L) {
      (clickCount - that.clickCount).toInt
    } else if ((orderCount - that.orderCount) > 0) {
      (orderCount - that.orderCount).toInt
    } else if ((payCount - that.payCount) > 0) {
      (payCount - that.payCount).toInt
    }
    0
}
```
# 4 topK聚合排序
- hive实现：行转列、开窗函数
```sql
val sql2 = "SELECT area, click_product_id, click_count,city_info, row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank," +
      "pi.product_name, if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      " FROM (" +
          " SELECT area, click_product_id, count(*) click_count, concat_ws(',', collect_set(city_info)) city_info FROM (" +
              " SELECT area, click_product_id, concat(city_id,':',city_name) city_info FROM tmp_click_product_basic " +
          ") t GROUP BY area,click_product_id" +
      ") a JOIN product_info pi ON a.click_product_id=pi.product_id 
```
- spark sql自定义函数实现
```scala
// 相同Execute间的数据合并。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufCityStr = buffer.getString(0)
    val inputCityStr = input.getString(0)

    if (!bufCityStr.contains(inputCityStr)) {
      bufCityStr += (if ("".equals(bufCityStr)) inputCityStr else "," + inputCityStr)
    }

    buffer.update(0, bufCityStr)
  }

  // 不同Execute间的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufCityStr1 = buffer1.getString(0)
    val bufCityStr2 = buffer2.getString(0)

    for (cityStr <- bufCityStr2.split(",") ) {
      if (!bufCityStr1.contains(cityStr)) {
        bufCityStr1 += (if ("".equals(bufCityStr1)) cityStr else "," + cityStr)
      }
    }

    buffer1.update(0, bufCityStr1)
  }
```
- RDD实现
```scala
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
```
# 实时业务
### 黑名单机制
```scala
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
```
### 实时流量统计
```scala
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
.............
```
### 实时流量趋势
 ```scala
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
```