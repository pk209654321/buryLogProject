package sparkAction.polarLightForAction

import bean.polarlight.PolarLightBeanNew
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import scalaUtil.HttpPostUtil

/**
  * ClassName PolarLightPost
  * Description TODO ,根据 Android,ios 传递post不同的地址
  * Author lenovo
  * Date 2019/5/18 11:10
  *
  **/
object PolarLightPost {

  def getHeaderAuthorization(device_type: String):String = {

    var authorization = ""
    var api_key = ""
    var api_secret = ""
    if (device_type.equals("Android")) {
      api_key = "97ec072bb99d4a59a4ffc0c65d1c3505"
      api_secret = "ef14cae275be44c8967afbdcd67badbd"
    }
    if (device_type.equals("iOS")) {
      api_key = "0845e8afcc114838ae0eaaeba1d4bbf1"
      api_secret = "e39c44d52bbd45d8ab327fe2de88cb24"
    }
    val temp = String.format("%s:%s", api_key, api_secret)
    import java.util.Base64
    val encrypt = Base64.getEncoder.encodeToString(temp.getBytes)
    authorization = String.format("Basic %s", encrypt)
    authorization
  }


  def getHeaderAuthorizationNew(device_type: String):String = {

    var authorization = ""
    var api_key = ""
    var api_secret = ""
    if (device_type.equals("Android")) {
      api_key = "c982c89945f54cef8d69d6b46fdb9923"
      api_secret = "19fefdb9a481408fbc2d9aa203229e82"
    }
    if (device_type.equals("iOS")) {
      api_key = "73821c7bc677482c8bf4875bbf5290a5"
      api_secret = "38d4f51a67614a44b716d79e671d44d4"
    }
    val temp = String.format("%s:%s", api_key, api_secret)
    import java.util.Base64
    val encrypt = Base64.getEncoder.encodeToString(temp.getBytes)
    authorization = String.format("Basic %s", encrypt)
    authorization
  }

  def postPolarLight(polarLightBean: PolarLightBean): Unit = {
    val device_id = polarLightBean.device_id
    val device_type = polarLightBean.device_type
    val conv_type = polarLightBean.conv_type
    var url_str = ""
    device_type match {
      case "Android" => url_str = s"http://gd-api.ad.jiguang.cn/v1/union-report/apps/98?device_id=$device_id&device_type=$device_type&conv_type=$conv_type"
      case "iOS" => url_str = s"http://gd-api.ad.jiguang.cn/v1/union-report/apps/99?device_id=$device_id&device_type=$device_type&conv_type=$conv_type"
      case _ =>
    }
    HttpPostUtil.sendMessageForPolarLight(url_str, "Authorization", getHeaderAuthorization(device_type))
  }


  def postPolarLightNew(polarLightBeanNew: PolarLightBeanNew): Boolean = {
    val device_type = polarLightBeanNew.getDevice_type
    val android_id = polarLightBeanNew.getAndroid_id
    val conv_type = polarLightBeanNew.getConv_type
    val device_id = polarLightBeanNew.getDevice_id
    val mac = polarLightBeanNew.getMac
    val jsonStr = JSON.toJSONString(polarLightBeanNew, SerializerFeature.WriteNullListAsEmpty)
    var url_str = ""
    device_type match {
      case "Android" => url_str = s"http://gd-api.ad.jiguang.cn/v2/union-report/apps/264"
      case "iOS" => url_str = s" http://gd-api.ad.jiguang.cn/v2/union-report/apps/265"
      case _ =>
    }
    HttpPostUtil.sendMessageForPolarLightNew(jsonStr, url_str, "Authorization", getHeaderAuthorizationNew(device_type))
  }

  def main(args: Array[String]): Unit = {
    //发起的数据:{"android_id":"9a92cdcce8ead4ca",
    // "conv_type":"APP_ACTIVE",
    // "device_id":"868097040164441",
    // "device_type":"Android",
    // "mac":"24:DA:33:5C:83:CC"}

    /*  val value = getHeaderAuthorization("Android")
      val urlStr = "http://gd-api.ad.jiguang.cn/v1/union-report/apps/98?device_id=861379030915736&device_type=Android&conv_type=APP_ACTIVE&android_id=9a92cdcce8ead4ca"
      HttpPostUtil.doHttpPost("", urlStr, "Authorization", value)*/
    //HttpPostUtil.sendPostMethod("",urlStr,"Authorization",value)

    val android_id = "9a92cdcce8ead4ca"
    val conv_type = "APP_ACTIVE"
    val device_id = "868097040164441"
    val device_type = "Android"
    val mac = "24:DA:33:5C:83:CC"
    val polarLightBeanNew = new PolarLightBeanNew()
    polarLightBeanNew.setAndroid_id(android_id)
    polarLightBeanNew.setConv_type(conv_type)
    polarLightBeanNew.setDevice_id(device_id)
    polarLightBeanNew.setDevice_type(device_type)
    polarLightBeanNew.setMac(mac)
    PolarLightPost.postPolarLightNew(polarLightBeanNew)
  }
}
