package testFunction.teacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TeacherTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TeacherTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val teacherRdd: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\teacher.log")
    val sbjectTeacherAndOne: RDD[(String, String)] = teacherRdd.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      (subject, teacher)
    })
  }
}
