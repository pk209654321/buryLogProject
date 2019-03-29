package sparkAction.PortfolioMysqlData

import bean.userChoiceStock.{PortGroupInfo, PortfolioBean}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.ProSecInfoList
import com.qq.tars.protocol.tars.BaseDecodeStream
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.{Portfolio, PortfolioStr}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

/**
  * ClassName PortfolioMysqlData
  * Description TODO
  * Author lenovo
  * Date 2019/3/29 14:51
  **/
object PortfolioMysqlData {

  //得到初始自选股数据
  def getPortfolioData(): List[(String, ProSecInfoList, String)] ={
    DBs.setupAll()
   val result= NamedDB('mysql).readOnly { implicit session =>
      SQL(s"select * from t_portfolio").map(rs => {
        val keyStr = rs.string("sKey")
        val valueStr = rs.bytes("sValue")
        val timeStr = rs.string("updatetime")
        val stream = new BaseDecodeStream(valueStr)
        val list = new ProSecInfoList()
        list.readFrom(stream)
        (keyStr,list,timeStr)
      }).list().apply()
    }
    result
  }

  def getPortfolioJsonStr(list:List[(String, ProSecInfoList, String)])={
    list.map(one=> {
      val sKey = one._1
      val sValue = one._2
      val time = one._3
      val jValue = JSON.toJSONString(sValue,SerializerFeature.WriteMapNullValue)
      (sKey,jValue,time)
    })
  }
  def getManyPortfolio(list:List[(String, ProSecInfoList, String)]):  List[ArrayBuffer[PortfolioBean]] ={
    val listPor = list.map(one => {
      val sKey = one._1
      val list = one._2
      val time = one._3
      val iVersion = list.iVersion
      val vProSecInfo = list.getVProSecInfo
      val portfolioBean = new PortfolioBean
      val array = new mutable.ArrayBuffer[PortfolioBean]()
      portfolioBean.setsKey(sKey)
      portfolioBean.setUpdateTime(time)
      portfolioBean.setiVersion(iVersion)
      if (vProSecInfo != null && vProSecInfo.size() > 0) {
        for (i <- 0 until (vProSecInfo.size())) {
          val info = vProSecInfo.get(i)
          val iCreateTime = info.getStCommentInfo.getICreateTime
          val iUpdateTime = info.getStCommentInfo.getIUpdateTime
          val sComment = info.getStCommentInfo.getSComment match {
            case "" => null
            case _ => info.getStCommentInfo.getSComment
          }
          var vBroadcastTime = info.getVBroadcastTime
//          match {
//            case null => null
//            case _ =>
//              val integers = JavaConversions.asScalaBuffer(info.getVBroadcastTime)
//              if (integers.length == 0) {
//                null
//              } else {
//                integers
//              }
//          }
          var vStrategyId = info.getVStrategyId
//          match {
//            case null => null
//            case _ =>
//              val integers: mutable.Seq[Integer] = JavaConversions.asScalaBuffer(info.getVStrategyId)
//              if (integers.length == 0) {
//                null
//              } else {
//                integers
//              }
//          }
          portfolioBean.setbRecvAnnounce(info.getBRecvAnnounce)
          portfolioBean.setbRecvResearch(info.getBRecvResearch)
          portfolioBean.setfChipHighPrice(info.getFChipHighPrice)
          portfolioBean.setfChipLowPrice(info.getFChipLowPrice)
          portfolioBean.setfDecreasesPer(info.getFDecreasesPer)
          portfolioBean.setfHighPrice(info.getFHighPrice)
          portfolioBean.setfIncreasePer(info.getFIncreasePer)
          portfolioBean.setfLowPrice(info.getFLowPrice)
          portfolioBean.setfMainChipHighPrice(info.getFMainChipHighPrice)
          portfolioBean.setfMainChipLowPrice(info.getFMainChipLowPrice)
          portfolioBean.setiCreateTime(info.getICreateTime)
          portfolioBean.setiUpdateTime(info.getIUpdateTime)
          portfolioBean.setsAiAlert(info.isAiAlert)
          portfolioBean.setsDel(info.isDel)
          portfolioBean.setsDKAlert(info.isDKAlert)
          portfolioBean.setsDtSecCode(info.getSDtSecCode)
          portfolioBean.setsHold(info.isHold)
          portfolioBean.setsName(info.getSName)
          portfolioBean.setStCommentInfo_iCreateTime(iCreateTime)
          portfolioBean.setStCommentInfo_iUpdateTime(iUpdateTime)
          portfolioBean.setStCommentInfo_sComment(sComment)
          portfolioBean.setvBroadcastTime(vBroadcastTime)
          portfolioBean.setvStrategyId(vStrategyId)

          array.+=(portfolioBean)

          portfolioBean.setbRecvAnnounce(null)
          portfolioBean.setbRecvResearch(null)
          portfolioBean.setfChipHighPrice(null)
          portfolioBean.setfChipLowPrice(null)
          portfolioBean.setfDecreasesPer(null)
          portfolioBean.setfHighPrice(null)
          portfolioBean.setfIncreasePer(null)
          portfolioBean.setfLowPrice(null)
          portfolioBean.setfMainChipHighPrice(null)
          portfolioBean.setfMainChipLowPrice(null)
          portfolioBean.setiCreateTime(null)
          portfolioBean.setiUpdateTime(null)
          portfolioBean.setsAiAlert(null)
          portfolioBean.setsDel(null)
          portfolioBean.setsDKAlert(null)
          portfolioBean.setsDtSecCode(null)
          portfolioBean.setsHold(null)
          portfolioBean.setsName(null)
          portfolioBean.setStCommentInfo_iCreateTime(null)
          portfolioBean.setStCommentInfo_iUpdateTime(null)
          portfolioBean.setStCommentInfo_sComment(null)
          portfolioBean.setvBroadcastTime(null)
          portfolioBean.setvStrategyId(null)
        }
      } else {
        array.+=(portfolioBean)
      }
      array
    })
    listPor
  }

  def getGroupPortfolio(list:List[(String, ProSecInfoList, String)]):  List[ArrayBuffer[PortGroupInfo]] ={
    val listGrop = list.map(one => {
      val sKey = one._1
      val list = one._2
      val time = one._3
      val iVersion = list.iVersion
      val groupInfo = list.getVGroupInfo
      val array = new mutable.ArrayBuffer[PortGroupInfo]()
      if (groupInfo != null && groupInfo.size > 0) {
        for (i <- 0 until (groupInfo.size())) {
          val gi = groupInfo.get(i)
          val portGroupInfo = new PortGroupInfo
          val gi_iCreateTime = gi.getICreateTime
          val gi_iUpdateTime = gi.getIUpdateTime
          val gi_sGroupName = gi.getSGroupName
          val gi_del = gi.isDel
          val gsList = gi.getVGroupSecInfo
          portGroupInfo.setGi_iCreateTime(gi_iCreateTime)
          portGroupInfo.setGi_isDel(gi_del)
          portGroupInfo.setGi_iUpdateTime(gi_iUpdateTime)
          portGroupInfo.setGi_sGroupName(gi_sGroupName)
          portGroupInfo.setiVersion(iVersion)
          portGroupInfo.setsKey(sKey)
          portGroupInfo.setUpdateTime(time)
          if (gsList != null && gsList.size() > 0) {
            for (j <- 0 until (gsList.size())) {
              val gsInfo = gsList.get(j)
              val gs_del = gsInfo.isDel
              val gs_iUpdateTime = gsInfo.getIUpdateTime
              val gs_sDtSecCode = gsInfo.getSDtSecCode


              portGroupInfo.setGs_isDel(gs_del)
              portGroupInfo.setGs_iUpdateTime(gs_iUpdateTime)
              portGroupInfo.setGs_sDtSecCode(gs_sDtSecCode)
              array.+=(portGroupInfo)
              portGroupInfo.setGs_isDel(null)
              portGroupInfo.setGs_iUpdateTime(null)
              portGroupInfo.setGs_sDtSecCode(null)
            }
          } else {
            array.+=(portGroupInfo)
          }
        }
      } else {
      }
      array
    })
    listGrop
  }

}
