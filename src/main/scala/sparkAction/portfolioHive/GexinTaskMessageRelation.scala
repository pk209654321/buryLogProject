package sparkAction.portfolioHive

import bean.ans.AnsFather
import com.alibaba.fastjson.JSON
import com.niufu.tar.bec.{PushControlData, PushData, PushType}
import conf.ConfigurationManager
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import sparkAction.{AnswerTable, AnswersBean, MsgContentData}

import scala.collection.mutable.ListBuffer


/**
  * ClassName NfRiskAssessmentUserCommitRecordToHive
  * Description TODO
  * Author lenovo
  * Date 2019/7/19 14:41
  **/

case class PushDataResultToHive(msg_id: String,
                                vtData:String,
                                ePushDataType:Int,
                                sBusinessId:String,
                                sMsgId:String,
                                iPushTime:Int,
                                iExpireTime:Int,
                                iStartTime:Int,
                                sTitle:String,
                                eDeviceType:Int,
                                sDescription:String,
                                iNotifyEffect:Int,
                                iRealPushType:Int,
                                sAndroidIconUrl:String,
                                sIosIconUrl:String,
                                iClassID:Int,
                                sDisplayType:String,
                                ePushNotificationType:Int,
                                iBuilderId:Int,
                                bDefaultCfg:Boolean
                               )

object GexinTaskMessageRelation {
  private val TABLERESULT: String = ConfigurationManager.getProperty("messagePushTable")


  def insertPushDataToHive(ansData: Dataset[MsgContentData], spark: SparkSession): Unit = {
    import spark.implicits._
    val dataFinal = ansData.map(one => {
      val temp=null
      val msg_id = one.msg_id
      try {
        val transmission_content = one.transmission_content
        var pushData = JSON.parseObject(transmission_content, classOf[PushData])
        pushData match {
          case null => pushData = new PushData()
          case _ =>
        }
        pushData.getStPushType match {
          case null => pushData.setStPushType(new PushType())
          case _ =>
        }
        pushData.getStControl match {
          case null => pushData.setStControl(new PushControlData())
          case _ =>
        }

        val strData = pushData.getVtData match {
          case null => null
          case _ => new String(pushData.getVtData,"utf-8")
        }
        PushDataResultToHive(msg_id match {
          case "" => temp
          case _ => msg_id
        },
          strData,
          pushData.getStPushType.getEPushDataType,
          pushData.getStPushType.getSBusinessId,
          pushData.getStPushType.getSMsgId,
          pushData.getIPushTime,
          pushData.getIExpireTime,
          pushData.getIStartTime,
          pushData.getSTitle,
          pushData.getEDeviceType,
          pushData.getSDescription,
          pushData.getINotifyEffect,
          pushData.getStControl.getIRealPushType,
          pushData.getStControl.getSAndroidIconUrl match {
            case "" => temp
            case _=>  pushData.getStControl.getSAndroidIconUrl
          },
          pushData.getStControl.getSIosIconUrl match {
            case "" => temp
            case _=> pushData.getStControl.getSIosIconUrl
          },
          pushData.getIClassID,
          pushData.getSDisplayType,
          pushData.getEPushNotificationType,
          pushData.getIBuilderId,
          pushData.getBDefaultCfg)
      } catch {
        case e:Throwable =>e.printStackTrace(); new PushDataResultToHive(msg_id,
          temp,
          0,
          temp,
          temp,
          0,
          0,
          0,
          temp,
          0,
          temp,
          0,
          0,
          temp,
          temp,
          0,
          temp,
          0,
          0,
          false)
      }
    })
    dataFinal.repartition(1).createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite table ${TABLERESULT} select * from tempTable")
  }

  def insertAnswerNew(ansData: Dataset[AnswersBean], spark: SparkSession) = {
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


  def getRealNum(num: String) = {
    val ints = new Array[Int](num.length)
    for (i <- (0 until num.length)) {
      ints(i) = String.valueOf(num.charAt(i)).toInt
    }
    ints
  }


  def main(args: Array[String]): Unit = {

  }
}