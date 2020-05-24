package com.z.biz.analyse.streaming

import java.util.{Properties, Random}
import com.z.biz.common.conf.ConfigurationManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.collection.mutable.ArrayBuffer

/**
 * kafka生成数据客户端
 */
object GenerateStreamData {

  /**
   * 创建kafka消息生产者
   *
   * @param broker
   * @return
   */
  def createProducer(broker: String): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //    properties.put(ProducerConfig.ACKS_CONFIG, "1") // defalut:1

    new KafkaProducer[String, String](properties)
  }

  /**
   * 模拟的数据
   * 时间点: 当前时间毫秒
   * userId: 0 - 99
   * 省份、城市 ID相同 ： 1 - 9
   * adid: 0 - 19
   * ((0L,"北京","北京"),(1L,"上海","上海"),(2L,"南京","江苏省"),(3L,"广州","广东省"),(4L,"三亚","海南省"),(5L,"武汉","湖北省"),
   * (6L,"长沙","湖南省"),(7L,"西安","陕西省"),(8L,"成都","四川省"),(9L,"哈尔滨","东北省"))
   * 格式 ：timestamp province city userid adid
   * 某个时间点 某个省份 某个城市 某个用户 某个广告
   */
  def generateStreamData(num: Int): Array[String] = {
    val array = ArrayBuffer[String]()
    val random = new Random()

    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to num) {
      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(10)
      val city = province
      val adid = random.nextInt(20)
      val userid = random.nextInt(100)

      // 拼接实时数据
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }

    array.toArray
  }

  /**
   * 查询topic列表：./kafka-topics --zookeeper node1:2181,node2:2181,node3:2181 --list
   * 创建topic：./kafka-topics --zookeeper node1:2181,node2:2181,node3:2181 --topic AdRealTimeLog --replication-factor 3 --partitions 3 --create
   * 生产者：./kafka-console-producer --broker-list node1:9092,node2:9092,node3:9092  --topic AdRealTimeLog
   * 消费者：./kafka-console-consumer --bootstrap-server node1:9092,node2:9092,node3:9092 --topic AdRealTimeLog --from-beginning
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val brokerList = ConfigurationManager.config.getString("kafka.broker.list")
    val topic = ConfigurationManager.config.getString("kafka.topics")

    val kafkaProducer = createProducer(brokerList)

    while (true) {

      for (item <- generateStreamData(50)) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, item))
      }

      Thread.sleep(5000)
    }

  }

}
