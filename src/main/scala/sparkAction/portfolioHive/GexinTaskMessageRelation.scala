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

case class PushDataResultToHive(msg_id:String,
                                gexin_task_id: String,
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
      val gexin_task_id = one.gexin_task_id match {
        case "" => temp
        case _ => one.gexin_task_id
      }
      val msg_id = one.msg_id match {
        case "" => null
        case _ => one.msg_id
      }
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
        PushDataResultToHive(
          msg_id,
          gexin_task_id,
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
        case e:Throwable =>e.printStackTrace(); new PushDataResultToHive(temp,temp,
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


  def main(args: Array[String]): Unit = {

  }
}