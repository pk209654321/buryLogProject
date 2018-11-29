package test

import java.io.File

import scala.io.Source

object Test1 {
  def main(args: Array[String]): Unit = {
    //    implicit def double2Int(a:Double)=a.toInt
    //    val b:Int=3.5
    //    println(b)
    //implicit def getRead(file:File)=new RichFile(file)
    //    val read: String = new File("").read
    val student1 = new Student("wangyadong")
    val student2: Student = new Student("zhangpeng")
  implicit def tempFun(stu:Student)=new Ordered[Student]{
    override def compare(that: Student): Int = {
      1
    }
  }
  val student: Student = compare(student1,student2)
  println(student.name)
}

  def compare[T](first:T,second:T)(implicit aa:T=>Ordered[T]) ={
    if(first>second){
      first
    }else{
      second
    }
  }
}

class RichFile(file: File) {
  def read = Source.fromFile(file).getLines().mkString
}


class Student(var name: String) {
  //将Student类的信息格式化打印
}

class OutputFormat(val first: String, val second: String)