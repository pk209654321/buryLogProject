package bean

import scala.beans.BeanProperty

/**
  * Created by lenovo on 2018/11/16.
  */
class StockShopClient {
  @BeanProperty var user_id:java.lang.Integer=null
  @BeanProperty var guid:String=null
  @BeanProperty var application:String=null
  @BeanProperty var version:String=null
  @BeanProperty var platform:String=null
  @BeanProperty var objectStr:String=null
  @BeanProperty var createtime:java.lang.Long=null
  @BeanProperty var action_type:java.lang.Integer=null
  @BeanProperty var is_fanhui:Integer=null
  @BeanProperty var scode_id:String=null
  @BeanProperty var market_id:String=null
  @BeanProperty var screen_direction:String=null
  @BeanProperty var color:String=null
  @BeanProperty var frameid:String=null
  @BeanProperty var typeInt:java.lang.Integer=null
  @BeanProperty var qs_id:String=null
  @BeanProperty var from_frameid:String=null
  @BeanProperty var from_object:String=null
  @BeanProperty var from_resourceid:String=null
  @BeanProperty var to_frameid:String=null
  @BeanProperty var to_resourceid:String=null
  @BeanProperty var to_scode:String=null
  @BeanProperty var target_id:String=null
}
