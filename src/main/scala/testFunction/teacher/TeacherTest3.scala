package testFunction.teacher

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TeacherTest3 {
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
    val strings: Array[String] = sbjectTeacherAndOne.map(_._1._1).distinct().collect()
    val reduceRdd: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(new ReducePartitioner(strings),_+_)
  }
}

class ReducePartitioner(array: Array[String]) extends Partitioner{

  var i=0
  private val hashMap = new mutable.HashMap[String,Int]()
  for (elem <- array) {
    hashMap.+=((elem,i))
    i+=0
  }
  override def numPartitions: Int = array.length

  override def getPartition(key: Any): Int = {
    val tuple: (String, String) = key.asInstanceOf[(String,String)]//强制类型转换
    hashMap(tuple._1)
  }
}
