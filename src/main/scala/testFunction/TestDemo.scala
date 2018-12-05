package testFunction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TestDemo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrayRdd: RDD[(String, Int, Int)] = sc.parallelize(Array(("王亚东",20,99),("王铮",66,55),("张跑",44,55)))
    ///implicit val p1=new TopImplitOrdring  //传递隐式参数,第一种方法
    import testFunction.scalaExample.other.OrderContext.topAction //第二种方法
    //第三种方法TopImplit 直接 extends Ordering/Ordered 实现compare方法
    val arraySort: RDD[(String, Int, Int)] = arrayRdd.sortBy(line => TopImplit(line._1,line._2,line._3))
    println(arraySort.collect().toBuffer)
  }
}


case class TopImplit(name:String,age:Int,value:Int)

//class TopImplitOrdring extends Ordering[TopImplit]{
//  override def compare(x: TopImplit, y: TopImplit): Int = {
//    if(x.value==y.value){
//      x.age-y.age
//    }else{
//      y.value-x.value
//    }
//  }
//}
