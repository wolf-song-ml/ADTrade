package com.z.biz.analyse.analog

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ConcatDistinct extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据类型
  override def inputSchema: StructType = StructType(StructField("citystr", StringType) :: Nil)

  // 聚合缓冲区中值得数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("bufCitystr", StringType) :: Nil)
  }

  // 返回值的数据类型
  override def dataType: StringType = StringType

  // 对于相同的输入是否一直返回相同的输出。
  override def deterministic: Boolean = true

  // 初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

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

  // 计算最终结果
  def evaluate(buffer: Row): String = buffer.getString(0)
}