# Spark大数据广告营销分析
## 项目目标
对电商网站的各种用户行为（访问行为、购物行为、广告点击行为等）进行复杂的分析。用统计分析出来的数据，辅助公司中的PM（产品经理）、数据分析师以及管理人员分析现有产品的情况，并根据用户行为分析结果持续改进产品的设计，以及调整公司的战略和业务。最终达到用大数据技术来帮助提升公司的业绩、营业额以及市场占有率的目标。


## 基础准备
###scala mysql连接池
`
/**
 * 对象池化工厂工具类
 *
 * @param jdbcUrl
 * @param jdbcUser
 * @param jdbcPassword
 * @param client
 */
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
`
### 日志切割
、、、
#按包输出到指定文件：Logfactory.getloger(clazz)
log4j.logger.com.z.biz.analyse.analog= ERROR, analyselog, stdout
log4j.additivity.com.z.biz.analyse.analog = false
log4j.appender.analyselog=org.apache.log4j.FileAppender
log4j.appender.analyselog.File=G://data//logs/biz-analyse.log
log4j.appender.analyselog.layout=org.apache.log4j.PatternLayout
log4j.appender.analyselog.layout.ConversionPattern=[%-5p] %d{yyyy-MM-dd HH:mm:ss.SSS} [%r] [%t] %l: %m %x %n
、、、