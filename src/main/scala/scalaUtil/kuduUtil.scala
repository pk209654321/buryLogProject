package scalaUtil

import org.apache.spark.sql.SparkSession

/**
  * ClassName kuduUtil
  * Description TODO
  * Author lenovo
  * Date 2019/9/18 20:45
  **/
object kuduUtil {
  def getKuduRow(array: Array[Any],spark:SparkSession) ={
    import spark.implicits._
    Seq(
      (10001,"uuuuuuuu",5555555,13999999999D,"fdsa","fdsa")
    ).toDF("s_suppkey", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")
  }
}
