package com.z.biz.test

import java.util.{Calendar, Date}

object BaseTest {
  def preDayLastHours(date: Date): Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, -1)
    cal.set(Calendar.MINUTE, 59)
    cal.set(Calendar.SECOND, 59)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }

  def main(args: Array[String]): Unit = {


    println(preDayLastHours(new Date()))
  }
}
