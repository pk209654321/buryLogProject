package hadoopCode.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RddFunction").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val par1: RDD[(String, Int)] = sc.parallelize(Array(("b",1),("b",2),("c",3),("gg",5)))
    val par2: RDD[(String, String)] = sc.parallelize(Array(("b","wang"),("b","ya"),("b","dong"),("gg","wansui")))
    val result: RDD[(String, Int)] = par1.aggregateByKey(0)((x:Int, y:Int)=>x+y, (m:Int, n:Int)=>m+n)
    val value: RDD[(String, List[String])] = par2.combineByKey((a:String)=>List[String](a), (x:List[String], y:String)=>y::x, (m:List[String], n:List[String])=>n:::m)
    println(value.collect().toBuffer)
  }
}
