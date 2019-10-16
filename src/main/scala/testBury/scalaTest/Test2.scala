package testBury.scalaTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName Test2
  * Description TODO
  * Author lenovo
  * Date 2019/10/10 10:22
  **/
object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TTT")
    val context = new SparkContext(sparkConf)
    val aaa = context.textFile("e://aaa.txt")
    aaa.collect().foreach(line=> {
      println(line)
    })
  }
}
