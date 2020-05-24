package com.z.biz.analyse.analog

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * 自定义累加器
 */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator

    // scala的private是实例可见性而Java的private是类的可见
    aggrStatMap.synchronized {
      newAcc.aggrStatMap ++= this.aggrStatMap
    }

    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  // add synchronized
  override def add(v: String): Unit = {
    aggrStatMap.synchronized {
      if (!aggrStatMap.contains(v))
        aggrStatMap += (v -> 0)
      //        aggrStatMap(v) = 0

      aggrStatMap.update(v, aggrStatMap(v) + 1)
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }

  // /: equal to fold.left and add synchronized
  /*
  def f (imap : mutable.HashMap[String, Int], tupple :(String, Int)) : mutable.HashMap[String, Int] = {
   tupple match {
     case (k, v)=>
       imap += (k -> (v + imap.getOrElse(k, 0)))
   }
  }
  acc.value.foldLeft(aggrStatMap)(f)
  */
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = other match {
    case acc: SessionAggrStatAccumulator => {
      acc.synchronized {
        (aggrStatMap /: acc.value) {
          case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0)))
        }
      }
    }
  }

}
