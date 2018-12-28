package hadoopCode.teacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TeacherTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TeacherTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val teacherRdd: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\teacher.log")
    val sbjectTeacherAndOne: RDD[((String, String),Int)] = teacherRdd.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher),1)
    })
    val reduceRdd: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)
    val groupRdd: RDD[(String, Iterable[((String, String), Int)])] = reduceRdd.groupBy(_._1._1)
    groupRdd.map(line => {
      val sub: String = line._1
      val it = line._2
      val tuples: List[((String, String), Int)] = it.toList.sortBy(_._2).reverse.take(3)
    })
  }
}
