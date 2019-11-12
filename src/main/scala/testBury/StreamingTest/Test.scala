package testBury.StreamingTest

/**
  * ClassName ExceptionMsgUtil
  * Description TODO
  * Author lenovo
  * Date 2019/9/11 15:52
  **/
object Test {
  def main(args: Array[String]): Unit = {
    val str ="aaaaa.bbbbbb"
    val strings = str.split("\\.")
    println(strings(0))
  }
}
