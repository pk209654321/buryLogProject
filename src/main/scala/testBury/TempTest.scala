package testBury

import scalaUtil.HttpPostUtil

/**
  * ClassName TempTest
  * Description TODO
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object TempTest {
  def main(args: Array[String]): Unit = {
   for(i <- 0 to 100){
     Thread.sleep(1000)
     println("开始发送")
     //HttpPostUtil.doHttpPost("","https://mp.weixin.qq.com/s/4oFXWuyLqFfg4P091ui68A")
   }
  }
}
