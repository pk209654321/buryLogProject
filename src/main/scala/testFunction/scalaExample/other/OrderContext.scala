package testFunction.scalaExample.other

import testFunction.TopImplit

object OrderContext {
  implicit val topAction  = new Ordering[TopImplit] {
    override def compare(x: TopImplit, y: TopImplit): Int = {
     if (x.value==y.value){
       x.age-y.age
     }else{
       y.value-x.value
     }
    }
  }


}


