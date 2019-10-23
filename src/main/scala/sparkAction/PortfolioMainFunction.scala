package sparkAction

import java.util

import bean.selectStock.ProSecInfoList
import bean.shareControl.{BlockInfo, ShareMany, SharePageInfo}
import bean.userChoiceStock.{PortGroupInfo, PortfolioBean}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.qq.tars.protocol.tars.BaseDecodeStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.PortfolioMysqlData.PortfolioMysqlDataObject
import sparkAction.portfolioHive.{NfRiskAssessmentUserCommitRecordToHive, PortfolioProSecInfoHiveInsertObject, UserPushButtonInsertToHive}

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}

/**
  * ClassName PortfolioMainFunction
  * Description TODO 用户自选股信息接入,分享控件信息接入,用户股票预警
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object PortfolioMainFunction {
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("spark.rpc.message.maxSize", "256")
    //sparkConf.set("spark.driver.extraJavaOptions", "-XX:PermSize=1g -XX:MaxPermSize=2g");
    sparkConf.set("spark.network.timeout", "3600")
    sparkConf.set("spark.debug.maxToStringFields", "100")
    if (LocalOrLine.isWindows) {
      sparkConf.setMaster("local[*]")
      println("----------------------------开发模式")
    }
    //val sc: SparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    val diffDay: Int = args(0).toInt
    val dataMysql = getMysqlDataNew(diffDay)
    //========================================================================================

    //========================================================================================

    //========================================================================================

    //========================================================================================

    //========================================================================================
    //用户自选股原始信息
    val portfolioStrs = getPortfolioFromMysql(dataMysql)
    val portfolias = spark.sparkContext.parallelize(portfolioStrs, 100)
    PortfolioProSecInfoHiveInsertObject.insertPortfolioToHive(portfolias, spark, diffDay)
    //自选股保存的证券信息
    val manyFieldPortfolio = getManyFieldPortfolio(dataMysql)
    val portfolioBeans: List[PortfolioBean] = manyFieldPortfolio.flatMap(_.toSeq)
    val many = spark.sparkContext.parallelize(portfolioBeans, 100)
    PortfolioProSecInfoHiveInsertObject.insertPortfolioManyToHive(many, spark, diffDay)
    //用户自选股组信息
    val groupInfo = getGroupInfoFromMysql(dataMysql)
    val portGroupInfoes = groupInfo.flatMap(_.toSeq)
    val groups = spark.sparkContext.parallelize(portGroupInfoes, 100)
    PortfolioProSecInfoHiveInsertObject.insertPortfolioToHiveGroupInfo(groups, spark, diffDay)
    //获取分享控件
    val shareManies = getShare.flatMap(_.toSeq)
    val shareRdds = spark.sparkContext.parallelize(shareManies, 1)
    PortfolioProSecInfoHiveInsertObject.insertShareControl(shareRdds, spark)
    //获取预警数据
    val userStockAlertCfgDataAlls = PortfolioMysqlDataObject.getWarnSeq()
    val userStockAlertCfgDataAllsRdd = spark.sparkContext.parallelize(userStockAlertCfgDataAlls, 20)
    PortfolioProSecInfoHiveInsertObject.insertEarlyWarn(userStockAlertCfgDataAllsRdd, spark)
    //获取用户开关数据
    val userPushData = UserPushButtonInsertToHive.getUserPushData()
    val userPushRdd = spark.sparkContext.parallelize(userPushData, 20)
    UserPushButtonInsertToHive.doUserPushButtonInsertToHive(userPushRdd, spark)

    spark.close()
  }

  //拉取masql 中的数据

  /*def getMysqlData(diffDay:Int) = {
    DBs.setupAll()
    val peoples = NamedDB('mysql).readOnly { implicit session =>
      SQL(s"select * from t_portfolio where ADDDATE(CURDATE(),INTERVAL  ${diffDay} DAY)=date(updatetime)").map(rs => Portfolio(rs.string("sKey"), rs.bytes("sValue"), rs.string("updatetime"))).list().apply()
    }
    peoples
  }*/

  /* def getMysqlDataAll(diffDay:Int) = {
     DBs.setupAll()
     val peoples = NamedDB('mysql).readOnly { implicit session =>
       SQL(s"select * from t_portfolio").map(rs =>

         Portfolio(rs.string("sKey"), rs.bytes("sValue"), rs.string("updatetime"))).list().apply()
     }
     peoples
   }*/

  def getMysqlDataNew(diffDay: Int) = {
    DBs.setupAll()
    val peoples = NamedDB('mysql).readOnly { implicit session =>
      SQL(s"select * from t_portfolio").map(rs => {
        val keyStr = rs.string("sKey")
        val valueStr = rs.bytes("sValue")
        val timeStr = rs.string("updatetime")
        val stream = new BaseDecodeStream(valueStr)
        val list = new ProSecInfoList()
        list.readFrom(stream)
        //val sValue = JSON.toJSONString(list, SerializerFeature.WriteMapNullValue)
        //Portfolio(rs.string("sKey"), rs.bytes("sValue"), rs.string("updatetime"))
        PortfolioList(keyStr, list, timeStr)
      }).list().apply()
    }
    peoples
  }

  def getShare() = {
    DBs.setupAll()
    val shareControls = NamedDB('share).readOnly { implicit session =>
      SQL("select * from scene_share_config").map(rs => ShareControl(rs.int("scene_code"), rs.string("share_page_info"))
      ).list().apply()
    }
    shareControls.map(line => {

      val scene_code = line.scene_code
      val share_page_info = line.share_page_info
      var sharePageInfo = new SharePageInfo
      try {
        sharePageInfo = JSON.parseObject(share_page_info, classOf[SharePageInfo])
      } catch {
        case e: Throwable => println("scene_code:" + scene_code + "----------------------------------" + share_page_info)
      }

      val list = sharePageInfo.getChannelList
      val array = new mutable.ArrayBuffer[ShareMany]()
      val shareMany = new ShareMany
      val bgcolor: Integer = sharePageInfo.getBgcolor
      val bgUrl: String = sharePageInfo.getBgUrl match {
        case "" => null
        case _ => sharePageInfo.getBgUrl
      }
      val downShareViewUrl: String = sharePageInfo.getDownShareViewUrl match {
        case "" => null
        case _ => sharePageInfo.getDownShareViewUrl
      }
      var middleBLock: BlockInfo = sharePageInfo.getMiddleBLock
      if (middleBLock != null) {
        shareMany.setMb_content(middleBLock.getContent match {
          case "" => null
          case _ => middleBLock.getContent
        })
        shareMany.setMb_contentLink(middleBLock.getContentLink match {
          case "" => null
          case _ => middleBLock.getContentLink
        })
        shareMany.setMb_textColor(middleBLock.getTextColor match {
          case "" => null
          case _ => middleBLock.getTextColor
        })
        shareMany.setMb_type(middleBLock.getType match {
          case "" => null
          case _ => middleBLock.getType
        })
      }
      val middleChar: String = sharePageInfo.getMiddleChar match {
        case "" => null
        case _ => sharePageInfo.getMiddleChar
      }
      val posterChar: String = sharePageInfo.getPosterChar match {
        case "" => null
        case _ => sharePageInfo.getPosterChar
      }
      val sceneCode: Integer = sharePageInfo.getSceneCode
      val topBlockLink: String = sharePageInfo.getTopBlockLink match {
        case "" => null
        case _ => sharePageInfo.getTopBlockLink
      }
      val topBlockUrl: String = sharePageInfo.getTopBlockUrl match {
        case "" => null
        case _ => sharePageInfo.getTopBlockUrl
      }
      val upShareViewUrl: String = sharePageInfo.getUpShareViewUrl match {
        case "" => null
        case _ => sharePageInfo.getUpShareViewUrl
      }
      //pageInfo中的详情
      shareMany.setBgcolor(bgcolor)
      shareMany.setBgUrl(bgUrl)
      shareMany.setMiddleChar(middleChar)
      shareMany.setTopBlockLink(topBlockLink)
      shareMany.setTopBlockUrl(topBlockUrl)
      shareMany.setDownShareViewUrl(downShareViewUrl)
      shareMany.setPosterChar(posterChar)
      shareMany.setSceneCode(sceneCode)
      shareMany.setUpShareViewUrl(upShareViewUrl)
      if (list != null && list.size() > 0) {
        for (i <- 0 until (list.size())) {
          //shareChannelInfo详情
          var shareMany2 = new ShareMany
          val shareChannelInfo = list.get(i)
          val code: Integer = shareChannelInfo.getCode
          val desc: String = shareChannelInfo.getDesc match {
            case "" => null
            case _ => shareChannelInfo.getDesc
          }
          val person: Integer = shareChannelInfo.getIsFirstPerson
          val permission: Integer = shareChannelInfo.getIsOutsidePermission
          val link: String = shareChannelInfo.getLink match {
            case "" => null
            case _ => shareChannelInfo.getLink
          }
          val name: String = shareChannelInfo.getName match {
            case "" => null
            case _ => shareChannelInfo.getName
          }
          val randomTitle: String = shareChannelInfo.getRandomTitle match {
            case "" => null
            case _ => shareChannelInfo.getRandomTitle
          }
          var randomTitleConfigs: util.List[String] = shareChannelInfo.getRandomTitleConfigs
          //          if (randomTitleConfigs != null && (randomTitleConfigs.size() == 0 || randomTitleConfigs.get(0).equals(""))) {
          //            randomTitleConfigs = null
          //          }
          if (randomTitleConfigs == null) {
            randomTitleConfigs = new util.ArrayList[String]()
          }
          val shareImgUrl: String = shareChannelInfo.getShareImgUrl match {
            case "" => null
            case _ => shareChannelInfo.getShareImgUrl
          }
          val shareTitle: String = shareChannelInfo.getShareTitle match {
            case "" => null
            case _ => shareChannelInfo.getShareTitle
          }
          val shareType: Integer = shareChannelInfo.getShareType

          shareMany2.setBgcolor(bgcolor)
          shareMany2.setBgUrl(bgUrl)
          shareMany2.setMiddleChar(middleChar)
          shareMany2.setTopBlockLink(topBlockLink)
          shareMany2.setTopBlockUrl(topBlockUrl)
          shareMany2.setDownShareViewUrl(downShareViewUrl)
          shareMany2.setPosterChar(posterChar)
          shareMany2.setSceneCode(sceneCode)
          shareMany2.setUpShareViewUrl(upShareViewUrl)

          shareMany2.setCode(code)
          shareMany2.setDesc(desc)
          shareMany2.setLink(link)
          shareMany2.setName(name)
          shareMany2.setPermission(permission)
          shareMany2.setPerson(person)
          shareMany2.setRandomTitle(randomTitle)
          shareMany2.setRandomTitleConfigs(randomTitleConfigs)
          shareMany2.setShareImgUrl(shareImgUrl)
          shareMany2.setShareTitle(shareTitle)
          shareMany2.setShareType(shareType)
          array += (shareMany2)
        }
      } else {
        array.+=(shareMany)
      }
      array
    })
  }

  def getGroupInfoFromMysql(dataMysql: scala.List[PortfolioList]) = {
    dataMysql.map(one => {
      val key = one.sKey
      val list = one.list
      val updatetime = one.updatetime
      //val stream = new BaseDecodeStream(value)
      //val list = new ProSecInfoList()
      //list.readFrom(stream)
      val iVersion = list.iVersion
      val groupInfo = list.getVGroupInfo
      val gip = groupInfo.toArray()
      val array = new mutable.ArrayBuffer[PortGroupInfo]()
      val groupInfoes = JavaConversions.asScalaBuffer(groupInfo)
      if (groupInfoes != null && groupInfoes.size > 0) {
        groupInfo.foreach(gi => {
          val portGroupInfo = new PortGroupInfo
          val gi_iCreateTime = gi.getICreateTime
          val gi_iUpdateTime = gi.getIUpdateTime
          val gi_sGroupName = gi.getSGroupName
          val gi_del = gi.isDel
          val gsList = gi.getVGroupSecInfo
          val groupSecInfoes = JavaConversions.asScalaBuffer(gsList)
          portGroupInfo.setGi_iCreateTime(gi_iCreateTime)
          portGroupInfo.setGi_isDel(gi_del)
          portGroupInfo.setGi_iUpdateTime(gi_iUpdateTime)
          portGroupInfo.setGi_sGroupName(gi_sGroupName)
          portGroupInfo.setiVersion(iVersion)
          portGroupInfo.setsKey(key)
          portGroupInfo.setUpdateTime(updatetime)
          portGroupInfo.setGi_lUptTimeExt(gi.getLUptTimeExt);
          if (groupSecInfoes != null && groupSecInfoes.size > 0) {
            groupSecInfoes.foreach(gsInfo => {
              var portGroupInfo2 = new PortGroupInfo
              val gs_del = gsInfo.isDel
              val gs_iUpdateTime = gsInfo.getIUpdateTime
              val gs_sDtSecCode = gsInfo.getSDtSecCode

              portGroupInfo2.setGi_iCreateTime(gi_iCreateTime)
              portGroupInfo2.setGi_isDel(gi_del)
              portGroupInfo2.setGi_iUpdateTime(gi_iUpdateTime)
              portGroupInfo2.setGi_sGroupName(gi_sGroupName)
              portGroupInfo2.setiVersion(iVersion)
              portGroupInfo2.setsKey(key)
              portGroupInfo2.setUpdateTime(updatetime)
              portGroupInfo2.setGi_lUptTimeExt(gi.getLUptTimeExt)


              portGroupInfo2.setGs_isDel(gs_del)
              portGroupInfo2.setGs_iUpdateTime(gs_iUpdateTime)
              portGroupInfo2.setGs_sDtSecCode(gs_sDtSecCode)
              portGroupInfo2.setGs_lUptTimeExt(gsInfo.getLUptTimeExt)
              array.+=(portGroupInfo2)
            })
          } else {
            array += (portGroupInfo)
          }
        })
      }
      array
    })
  }

  def getPortfolioFromMysql(dataMysql: scala.List[PortfolioList]): List[PortfolioStr] = {
    val portfolioStrs = dataMysql.map(one => {
      val sValue = JSON.toJSONString(one.list, SerializerFeature.WriteMapNullValue)
      PortfolioStr(one.sKey, sValue, one.updatetime)
    })
    portfolioStrs
  }

  def getManyFieldPortfolio(dataMysql: scala.List[PortfolioList]) = {
    val listTup = dataMysql.map(one => {
      val list = one.list
      val iVersion = list.iVersion
      val vProSecInfo = list.getVProSecInfo
      val portfolioBean = new PortfolioBean
      val array = new mutable.ArrayBuffer[PortfolioBean]()
      portfolioBean.setsKey(one.sKey)
      portfolioBean.setUpdateTime(one.updatetime)
      portfolioBean.setiVersion(iVersion)
      if (vProSecInfo != null && vProSecInfo.size() > 0) {
        for (i <- 0 until (vProSecInfo.size())) {
          var portfolioBean2 = new PortfolioBean
          val info = vProSecInfo.get(i)
          val iCreateTime = info.getStCommentInfo.getICreateTime
          val iUpdateTime = info.getStCommentInfo.getIUpdateTime
          val sComment = info.getStCommentInfo.getSComment match {
            case "" => null
            case _ => info.getStCommentInfo.getSComment
          }
          var vBroadcastTime = info.getVBroadcastTime
          var vStrategyId = info.getVStrategyId
          if (vBroadcastTime == null) {
            vBroadcastTime = new util.ArrayList[Integer]()
          }
          if (vStrategyId == null) {
            vStrategyId = new util.ArrayList[Integer]()
          }
          portfolioBean2.setsKey(one.sKey)
          portfolioBean2.setUpdateTime(one.updatetime)
          portfolioBean2.setiVersion(iVersion)

          portfolioBean2.setbRecvAnnounce(info.getBRecvAnnounce)
          portfolioBean2.setbRecvResearch(info.getBRecvResearch)
          portfolioBean2.setfChipHighPrice(info.getFChipHighPrice)
          portfolioBean2.setfChipLowPrice(info.getFChipLowPrice)
          portfolioBean2.setfDecreasesPer(info.getFDecreasesPer)
          portfolioBean2.setfHighPrice(info.getFHighPrice)
          portfolioBean2.setfIncreasePer(info.getFIncreasePer)
          portfolioBean2.setfLowPrice(info.getFLowPrice)
          portfolioBean2.setfMainChipHighPrice(info.getFMainChipHighPrice)
          portfolioBean2.setfMainChipLowPrice(info.getFMainChipLowPrice)
          portfolioBean2.setiCreateTime(info.getICreateTime)
          portfolioBean2.setiUpdateTime(info.getIUpdateTime)
          portfolioBean2.setsAiAlert(info.isAiAlert)
          portfolioBean2.setsDel(info.isDel)
          portfolioBean2.setsDKAlert(info.isDKAlert)
          portfolioBean2.setsDtSecCode(info.getSDtSecCode match {
            case "" => null
            case _ => info.getSDtSecCode
          })
          portfolioBean2.setsHold(info.isHold)
          portfolioBean2.setsName(info.getSName match {
            case "" => null
            case _ => info.getSName
          })
          portfolioBean2.setStCommentInfo_iCreateTime(iCreateTime)
          portfolioBean2.setStCommentInfo_iUpdateTime(iUpdateTime)
          portfolioBean2.setStCommentInfo_sComment(sComment)
          portfolioBean2.setvBroadcastTime(vBroadcastTime)
          portfolioBean2.setvStrategyId(vStrategyId)
          portfolioBean2.setlUptTimeExt(info.getLUptTimeExt)
          portfolioBean2.setbInitiative(info.getBInitiative)
          array.+=(portfolioBean2)

        }
      } else {
        array.+=(portfolioBean)
      }
      array
    })
    listTup
  }
}

case class PortfolioList(var sKey: String, var list: ProSecInfoList, var updatetime: String)

case class PortfolioStr(var sKey: String, var sValue: String, var updatetime: String)

case class ShareControl(scene_code: Int, share_page_info: String)
