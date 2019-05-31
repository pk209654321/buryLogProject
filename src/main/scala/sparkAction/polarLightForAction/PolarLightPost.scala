package sparkAction.polarLightForAction

import scalaUtil.HttpPostUtil

/**
  * ClassName PolarLightPost
  * Description TODO ,根据 Android,ios 传递post不同的地址
  * Author lenovo
  * Date 2019/5/18 11:10
  *
  **/
object PolarLightPost {

  def getHeaderAuthorization(device_type:String)={

    var authorization=""
    var api_key=""
    var api_secret=""
    if(device_type.equals("Android")){
      api_key="97ec072bb99d4a59a4ffc0c65d1c3505"
      api_secret="ef14cae275be44c8967afbdcd67badbd"
    }
    if(device_type.equals("iOS")){
      api_key="0845e8afcc114838ae0eaaeba1d4bbf1"
      api_secret="e39c44d52bbd45d8ab327fe2de88cb24"
    }
    val temp = String.format("%s:%s", api_key, api_secret)
    import java.util.Base64
    val encrypt = Base64.getEncoder.encodeToString(temp.getBytes)
    authorization = String.format("Basic %s", encrypt)
    authorization
  }

  def postPolarLight(polarLightBean:PolarLightBean): Unit ={
    val device_id = polarLightBean.device_id
    val device_type = polarLightBean.device_type
    val conv_type = polarLightBean.conv_type
    var url_str=""
    device_type match {
      case "Android" => url_str=s"http://gd-api.ad.jiguang.cn/v1/union-report/apps/98?device_id=${device_id}&device_type=${device_type}&conv_type=${conv_type}"
      case "iOS" =>url_str=s"http://gd-api.ad.jiguang.cn/v1/union-report/apps/99?device_id=${device_id}&device_type=${device_type}&conv_type=${conv_type}"
      case _ =>
    }
    HttpPostUtil.sendMessageForPolarLight(url_str,"Authorization",getHeaderAuthorization(device_type))
}

  def main(args: Array[String]): Unit = {
    val value = getHeaderAuthorization("Android")
    println(value)
    val urlStr="http://gd-api.ad.jiguang.cn/v1/union-report/apps/98?device_id=861379030915736&device_type=Android&conv_type=APP_ACTIVE&conv_time=1514739661"
    HttpPostUtil.doHttpPost("",urlStr,"Authorization",value)
  }
}
