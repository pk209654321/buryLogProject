package bean

import scala.beans.BeanProperty

/**
  * Created by lenovo on 2018/11/21.
  */
class StockShopVisit {
  @BeanProperty var user_id:java.lang.Integer=null
  @BeanProperty var guid:String=null
  @BeanProperty var access_time:java.lang.Long=null
  @BeanProperty var offline_time:java.lang.Long=null
  @BeanProperty var download_channel:String=null
  @BeanProperty var client_version:String=null
  @BeanProperty var phone_model:String=null
  @BeanProperty var phone_system:String=null
  @BeanProperty var system_version:String=null
  @BeanProperty var operator:String=null
  @BeanProperty var network:String=null
  @BeanProperty var resolution:String=null
  @BeanProperty var screen_height:String=null
  @BeanProperty var screen_width:String=null
  @BeanProperty var mac:String=null
  @BeanProperty var ip:String=null
  @BeanProperty var imei:String=null
  @BeanProperty var iccid:String=null
  @BeanProperty var meid:String=null
  @BeanProperty var idfa:String=null
}
