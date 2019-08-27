package sparkAction.portfolioHive

import java.util

import bean.ans.{AnsFather, AnsItem}
import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaUtil.StructUtil
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.{AnswerTable, AnswersBean}

import scala.collection.mutable.ListBuffer


/**
  * ClassName NfRiskAssessmentUserCommitRecordToHive
  * Description TODO
  * Author lenovo
  * Date 2019/7/19 14:41
  **/

object NfRiskAssessmentUserCommitRecordToHive {
  private val TABLENAME: String = ConfigurationManager.getProperty("userAnswer")
  private val TABLERESULT: String = ConfigurationManager.getProperty("userAnswerResult")

  def getUserAnsFromMysql(): scala.List[(Long, String, String,Long)] = {
    DBs.setupAll()
    val userText = NamedDB('answer).readOnly {
      val sql = s"select  ID,ACCOUNT_ID,USER_ANS,UPDATE_TIME from ${TABLENAME}"
      implicit session =>
        SQL(sql).map(rs => {
          val user_id = rs.string("ACCOUNT_ID")
          val str = rs.string("USER_ANS")
          val id = rs.long("ID")
          val update_time = rs.long("UPDATE_TIME")
          (id,user_id, str,update_time)
        }).list().apply()
    }
    userText
  }

  def insertAnswerNew( ansData: Dataset[AnswersBean],spark:SparkSession) ={
    import spark.implicits._
    val dataRow = ansData.map(one => {
      val id = one.ID
      val user_id = one.ACCOUNT_ID
      val userans = one.USER_ANS
      val update_time = one.UPDATE_TIME
      try {
        val father = JSON.parseObject(userans, classOf[AnsFather])
        val items = father.getvAnsItem()
        val buffer = new ListBuffer[String]()
        for (i <- (0 until items.size())) {
          val ansItem = items.get(i)
          val queNo = ansItem.getiQuestionNo()
          val ansNo = ansItem.getsAnswer()
          val options = ansItem.getvQuestionOption()
          if (options.isEmpty) { //如果答题相为空则为str类型答案 例如:good
            buffer.+=(ansNo)
          } else {
            //得到answer数组:1234
            val ints = getRealNum(ansNo)
            var sub: String = "";
            for (i <- (0 until (ints.length))) {
              if (i == ints.length - 1) {
                sub += options.get(ints(i) - 1).getsTitle()
              } else {
                sub += options.get(ints(i) - 1).getsTitle() + "|"
              }
            }
            buffer.+=(sub)
          }
        }
        AnswerTable(id, user_id, buffer, update_time)
      } catch {
        case t: Throwable => t.printStackTrace(); AnswerTable(id, user_id, null, update_time)
      }
    })
    dataRow.repartition(1).createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite  table ${TABLERESULT}  select * from tempTable")
  }

  def insertAnswerToHive(answerRdd: RDD[(Long, String, String,Long)], spark: SparkSession): Unit = {
    val ansRow = answerRdd.map(line => {

      val id = line._1
      val user_id = line._2
      val txt = line._3
      val update_time = line._4
      try {
      val father = JSON.parseObject(txt, classOf[AnsFather])
      val items = father.getvAnsItem()
      val buffer = new ListBuffer[String]()
      for (i <- (0 until items.size())) {
        val ansItem = items.get(i)
        val queNo = ansItem.getiQuestionNo()
        val ansNo = ansItem.getsAnswer()
        val options = ansItem.getvQuestionOption()
        if (options.isEmpty) {
          buffer.+=(ansNo)
        } else {
          val ints = getRealNum(ansNo)
          var sub:String="";
          for(i <- (0 until(ints.length))){
            if(i==ints.length-1){
              sub+=options.get(ints(i)-1).getsTitle()
            }else{
              sub+=options.get(ints(i)-1).getsTitle()+"|"
            }
          }
          buffer.+=(sub)
        }
      }
        Row(id,user_id, buffer,update_time)
      } catch {
        case e: Throwable =>e.printStackTrace();println(line._3);Row(id,null,null,update_time)
      }
    })
    val createDataFrame = spark.createDataFrame(ansRow, StructUtil.structAnswer).repartition(1)
    createDataFrame.createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite  table ${TABLERESULT}  select * from tempTable")
  }


  def getRealNum(num: String) = {
    /**
    　　* @Description: TODO  将字符串数字转换成数字数组
    　　* @param [num]
    　　* @return int[]
    　　* @throws
    　　* @author lenovo
    　　* @date 2019/7/30 16:58
    　　*/
    val ints = new Array[Int](num.length)
    for (i <- (0 until num.length)) {
      ints(i) = String.valueOf(num.charAt(i)).toInt
    }
    ints
  }


  def main(args: Array[String]): Unit = {
    val mysql = getUserAnsFromMysql
    println(mysql)
  }
}