package com.z.biz.common.utils

import com.alibaba.fastjson.JSONObject


/**
 * 参数工具类
 *
 */
object ParamUtils {

  /**
   * 从JSON对象中提取参数
   *
   * @param jsonObject JSON对象
   * @return 参数
   */
  def getParam(jsonObject: JSONObject, field: String): String = {
    jsonObject.getString(field)
    /*val jsonArray = jsonObject.getJSONArray(field)
    if(jsonArray != null && jsonArray.size() > 0) {
      return jsonArray.getString(0)
    }
    null*/
  }

}