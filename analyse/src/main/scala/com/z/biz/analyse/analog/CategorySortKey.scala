package com.z.biz.analyse.analog

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


}
