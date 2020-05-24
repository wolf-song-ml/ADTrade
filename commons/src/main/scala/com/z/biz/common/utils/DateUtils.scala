package com.z.biz.common.utils

import java.text.ParseException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.Period
import java.util
import java.util.{ArrayList, Calendar, Collections, Date, List}
import java.util.regex.Pattern

object DateUtils {
  val YYYY = "yyyy"
  val YYYYMM = "yyyyMM"
  val YYYYMMDD = "yyyyMMdd"
  val YYYYMMDD_ = "yyyy-MM-dd"
  val yyyyMMddHHmmss = "yyyyMMddHHmmss"
  val yyyyMMddHHmmss_ = "yyyy-MM-dd HH:mm:ss"
  val yyyyMMddHHmmssSSS_ = "yyyy-MM-dd HH:mm:ss:SSS"
  val HHmmssSSS_ = "HH:mm:ss:SSS"
  val yyyyMMddHHmmssSSS = "yyyyMMddHHmmssSSS"
  val yyyyMdHms_ = "yyyy-M-d H:m:s"
  val yyMdHms = "yy-M-d H:m:s"
  val yyMM = "yyMM"
  val yyyyMMddHHmm_ = "yyyy-MM-dd HH:mm"
  val HHmm = "HH:mm"
  val yyyyMMddp = "yyyy.MM.dd"
  val MMdd = "MM月dd日"
  val YYYYMMDDHHMMSS = "yyyy年MM月dd日HH时mm分ss秒"

  private val DAY = 86400000
  private val HOURS = 3600000
  private val MINUTES = 60000
  private val SECONDS = 1000

  @throws(classOf[ParseException])
  def stringToDate(pattern: String, date: String): Date = {
    val format = new SimpleDateFormat(pattern)
    format.parse(date)
  }

  /**
   * 日期转换为字符串
   *
   * @param pattern
   * @param date
   * @return
   */
  def dateToString(pattern: String, date: Date): String = {
    val format = new SimpleDateFormat(pattern)
    format.format(date)
  }

  /**
   * 计算两个时间相差几小时
   *
   * @param beginDate
   * @param endDate
   * @return
   */
  def timeDiff(beginDate: Date, endDate: Date): Double = {
    val hours = (endDate.getTime - beginDate.getTime).toDouble / 3600 / 1000
    hours
  }

  /**
   * 计算两个时间相差几分钟
   *
   * @param beginDate
   * @param endDate
   * @return
   */
  @throws[Exception]
  def timeDiff(beginDate: String, endDate: Date): Int = {
    val bDate = stringToDate(yyyyMMddHHmmss_, beginDate)
    val minutes = ((endDate.getTime - bDate.getTime) / 60 / 1000).toInt
    minutes
  }

  /**
   * 计算两个时间相差几分钟
   *
   * @param beginDate 起始日期
   * @param endDate   结束日期
   * @return
   */
  @throws[Exception]
  def hourMinutesComp(beginDate: Date, endDate: Date): Int = {
    val minutes = ((endDate.getTime - beginDate.getTime) / 60 / 1000).toInt
    minutes
  }

  /**
   * 计算两个时间相差秒钟数
   *
   * @param beginDate
   * @param endDate
   * @return
   */
  def timeDiffBySecond(beginDate: String, endDate: String): Int = {
    val aDate = stringToDate(yyyyMMddHHmmss_, endDate)
    val bDate = stringToDate(yyyyMMddHHmmss_, beginDate)
    val totalSecond = ((aDate.getTime - bDate.getTime) / 1000).toInt
    totalSecond
  }

  def timeDiffBySecond(beginDate: Date, endDate: Date): Int = {
    val totalSecond = ((endDate.getTime - beginDate.getTime) / 1000).toInt
    totalSecond
  }

  /**
   * 时间字符串格式化
   *
   * @param leftTime
   * @return
   */
  def time2Str(leftTime: Long): String = {
    val buff = new StringBuilder(32)
    var count: Long = 0
    var left: Long = 0

    if (leftTime > DAY) {
      count = left / DAY
      left = leftTime - (count * DAY)
      buff.append(count + "天")
    }

    if (left > HOURS) {
      count = left / HOURS
      left = left - (count * HOURS)
      buff.append(count + "小时")
    }

    if (leftTime > MINUTES) {
      count = left / MINUTES
      left = left - (count * MINUTES)
      buff.append(count + "分")
    }

    if (left > SECONDS) {
      count = left / SECONDS
      buff.append(count + "秒")
    }

    buff.toString
  }

  def minute2DHS(leftTime: Long): String = {
    val buff = new StringBuffer(32)
    var count: Long = 0
    var left: Long = 0

    if (leftTime > 60 * 24) {
      count = leftTime / (60 * 24)
      left = leftTime - (count * (60 * 24))
      buff.append(count + "天")
    }

    if (left > 60) {
      count = left / 60
      left = left - (count * 60)
      buff.append(count + "小时")
    }

    if (leftTime >= 0) buff.append(leftTime + "分钟")
    buff.toString
  }

  /**
   * 两个日期相差天数，严格按照时分秒计算
   *
   * @param fDate 开始日期
   * @param oDate 结束日期
   * @return
   */
  def getIntervalDaysBy(fDate: Date, oDate: Date): Int = {
    if (null == fDate || null == oDate) return -1
    val intervalMilli = oDate.getTime - fDate.getTime
    (intervalMilli / (24 * 60 * 60 * 1000)).toInt
  }

  /**
   * 两个日期相差天数，仅按照日期计算
   *
   * @param fDate 开始日期
   * @param oDate 结束日期
   * @return
   */
  def getIntervalDays(fDate: Date, oDate: Date): Int = {
    val aCalendar = Calendar.getInstance
    aCalendar.setTime(fDate)
    val time1 = aCalendar.getTimeInMillis

    aCalendar.setTime(oDate)
    val time2 = aCalendar.getTimeInMillis

    val betweenDays = (time2 - time1) / (1000 * 3600 * 24)
    betweenDays.toInt
  }

  /**
   * 获取一个时间的第二天零点零分零秒
   *
   * @param date
   * @return
   */
  def nextDayHours(date: Date): Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DAY_OF_MONTH, 1)
    cal.getTime
  }

  /**
   * 获取当前时间的零点零分零秒
   *
   * @param date
   * @return
   */
  def currentDayHours(date: Date): Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }

  /**
   * 获取前一天的23点59分59秒
   *
   * @param date
   * @return
   */
  def preDayLastHours(date: Date): Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, -1)
    cal.set(Calendar.MINUTE, 59)
    cal.set(Calendar.SECOND, 59)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }

  /**
   * 当前时间+n天
   *
   * @param date
   * @return
   */
  def datesAddDays(date: Date, days: Int): Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.add(Calendar.DATE, days)
    cal.getTime
  }

  /**
   * 当前时间+n年
   *
   * @param date
   * @return
   */
  def datesAddYears(date: Date, years: Int): Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.add(Calendar.YEAR, years)
    cal.getTime
  }

  /**
   * 根据某个日期 获取该日期所在周的所有日期
   *
   * @param date yyyy-MM-dd
   * @return 从星期一开始到星期天
   */
  //  @throws(classOf[ParseException])
  def getDatesForWeek(date: String): util.List[String] = {
    val list = new util.ArrayList[String]
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance

    if (date != null && !date.isEmpty) {
      var time: Date = null
      try {
        time = sdf.parse(date)
      } catch {
        case e: ParseException => time = null
      }
      cal.setTime(time)
    }

    // 从星期天开始为1~周六7
    val dayWeek = cal.get(Calendar.DAY_OF_WEEK)
    if (dayWeek == 1) {
      cal.add(Calendar.DATE, -7)
    } else {
      cal.add(Calendar.DATE, -(6 - (7 - dayWeek)))
    }

    for (i <- 1 to 7) {
      cal.add(Calendar.DATE, 1)
      list.add(sdf.format(cal.getTime))
    }

    list
  }

  /**
   * 获取一周日期（默认当前日期的一周日期）
   *
   * @return
   */
  def getDatesForWeek(): util.List[String] = {
    val list = new util.ArrayList[String]
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance
    cal.setTime(new Date)

    val dayWeek = cal.get(Calendar.DAY_OF_WEEK)

    if (dayWeek == 1) {
      cal.add(Calendar.DATE, -7)
    } else {
      cal.add(Calendar.DATE, -(6 - (7 - dayWeek)))
    }

    for (i <- 1 to 7) {
      cal.add(Calendar.DATE, 1)
      list.add(sdf.format(cal.getTime))
    }

    list
  }

  /**
   * 根据日期获取该日期属于星期几
   *
   * @param date 日期
   * @return
   */
  def getDayOfWeek(date: Date): Int = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_WEEK) - 1
  }

  /**
   * 根据传入的日期段 获取该日期段内的所有日期
   *
   * @param startString endString  yyyy-MM-dd
   * @return 从起始日期开始到终止日期
   */
  def getDatesForDays(startString: String, endString: String): util.List[String] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val list = new util.ArrayList[String]
    try {
      val startDate = sdf.parse(startString)
      val endDate = sdf.parse(endString)
      val cd = Calendar.getInstance

      var itDate = startDate
      while (itDate.getTime <= endDate.getTime) {
        list.add(sdf.format(itDate))

        cd.setTime(itDate)
        cd.add(Calendar.DATE, 1)

        itDate = cd.getTime
      }
    } catch {
      case e: ParseException => e.printStackTrace()
    }

    list
  }

  /**
   * @Description:由出生日期获得年龄
   * @Param:[birthDay, nowDate] 出生日期 现在的日期
   * @return:int
   */
  def getAge(birthDay: Date, nowDate: Date): Int = {
    var age = 0
    try {
      val cal = Calendar.getInstance
      cal.setTime(nowDate)

      if (cal.before(birthDay))
        throw new IllegalArgumentException("出生日期比当前日期还大!")

      val yearNow = cal.get(Calendar.YEAR)
      val monthNow = cal.get(Calendar.MONTH)
      val dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH)

      cal.setTime(birthDay)
      val yearBirth = cal.get(Calendar.YEAR)
      val monthBirth = cal.get(Calendar.MONTH)
      val dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH)

      age = yearNow - yearBirth
      if (monthNow == monthBirth) {
        age = if (dayOfMonthNow < dayOfMonthBirth) age - 1 else age
      } else if (monthNow < monthBirth) {
        age -= 1
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }

    age
  }

  val DATE_REGX: String = "^(((20[0-9][0-9]-(0[1-9]|1[02]|[1-9])" + "-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]" +
    "-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) " + "(20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])$"
  val DATE_Y_M_D_REGX = "^(((20[0-9][0-9]-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]))))$"

  /**
   * 验证日期 yyy-MM-dd hh:mm:ss
   *
   * @param date
   * @return true 通过，false 不过
   * @author by lwq
   */
  def checkDate(date: String): Boolean = {
    if (StringUtils.isEmpty(date)) return false
    Pattern.matches(DATE_REGX, date)
  }

  /**
   *
   * @param date
   * @return
   */
  def checkYearMonthDay(date: String): Boolean = {
    if (StringUtils.isEmpty(date)) return false
    Pattern.matches(DATE_Y_M_D_REGX, date)
  }

  /**
   * 比较两个时间大小
   *
   * @param date1
   * @param date2
   * @return
   */
  def compareDate(date1: String, date2: String): Int = {
    val df = new SimpleDateFormat(yyyyMMddHHmmss_)
    try {
      val dt1 = df.parse(date1)
      val dt2 = df.parse(date2)
      if (dt1.getTime > dt2.getTime) {
        return 1
      } else if (dt1.getTime < dt2.getTime) {
        return -1
      } else
        return 0
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
    0
  }

  /**
   * 一组数据是星期几
   *
   * @param dates
   * @return
   */
  def getWeeksNum(dates: util.List[String]): util.List[Long] = {
    val weeks = new util.ArrayList[Long]
    import scala.collection.JavaConversions._
    for (date <- dates) {
      weeks.add(getWeek(date))
    }
    weeks
  }

  /**
   * 根据日期获取星期几
   *
   * @param dates
   * @return
   */
  def getWeeksStr(dates: util.List[String]): util.List[String] = {
    val dayNames = Array("星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六")
    val weeks = new util.ArrayList[String]

    import scala.collection.JavaConversions._
    for (date <- dates) {
      weeks.add(dayNames(getWeek(date)))
    }
    weeks
  }

  /**
   * 获取星期几
   *
   * @param date
   * @return
   */
  def getWeek(date: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance

    try {
      calendar.setTime(sdf.parse(date))
    } catch {
      case e: ParseException => e.printStackTrace()
    }

    calendar.get(Calendar.DAY_OF_WEEK) - 1
  }

  /**
   * 获取未来或过去几天的日期集合
   *
   * @param startDate
   * @param past
   * @param incrementVal
   * @return
   */
  def getDates(startDate: Date, past: Int, incrementVal: Int): util.List[String] = {
    val fetureDaysList = new util.ArrayList[String]
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance

    if (startDate == null) {
      calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR))
    } else
      calendar.setTime(startDate)

    for (i <- 0 until past) {
      val result = sdf.format(calendar.getTime)
      calendar.add(Calendar.DATE, incrementVal)
      fetureDaysList.add(result)
    }

    if (incrementVal < 0) Collections.reverse(fetureDaysList)

    fetureDaysList
  }

  /**
   * 计算2个日期之间相差的  相差多少年月日
   * 比如：2011-02-02 到  2017-03-02 相差 6年，1个月，0天
   *
   * @param fromDate YYYY-MM-DD
   * @param toDate   YYYY-MM-DD
   * @return 年,月,日 例如 1,1,1
   */
  def dayComparePrecise(fromDate: String, toDate: String): String = {
    val period = Period.between(LocalDate.parse(fromDate), LocalDate.parse(toDate))
    val sb = new StringBuffer
    sb.append(period.getYears).append(",").append(period.getMonths).append(",").append(period.getDays)
    sb.toString
  }

  /**
   * 计算2个日期之间相差的  相差多少年月日
   *
   * @param fromDate
   * @param toDate
   * @return
   */
  def dayComparePrecise(fromDate: Date, toDate: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val period = Period.between(LocalDate.parse(sdf.format(fromDate)), LocalDate.parse(sdf.format(toDate)))
    val sb = new StringBuffer()
    sb.append(period.getYears).append(",").append(period.getMonths).append(",").append(period.getDays)
    sb.toString
  }

  /**
   * 获取年月日和小时
   *
   * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
   * @return 结果（yyyy-MM-dd_HH）
   */
  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

}