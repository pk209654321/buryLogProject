package testBury

import hadoopCode.hbaseCommon.HBaseUtil
import scalaUtil.HttpPostUtil

/**
  * ClassName TempTest
  * Description TODO
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object TempTest {
  def main(args: Array[String]): Unit = {
    val line="access_time="
    val strings = line.split("=")
    println(strings.size)
  }
}
