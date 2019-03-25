package testBury.beanTest

import java.io.{File, PrintWriter}

import bean.PortfolioBean
import scalaUtil.DateScalaUtil
import sparkAction.PortfolioMainFunction.{getGroupInfoFromMysql, getManyFieldPortfolio}
import sparkAction.{Portfolio, PortfolioMainFunction, PortfolioStr}

import scala.io.Source

/**
  * ClassName JsonForTest
  * Description TODO
  * Author lenovo
  * Date 2019/3/21 9:43
  **/
object JsonForTest {
  def main(args: Array[String]): Unit = {
    val portfolios = PortfolioMainFunction.getMysqlDataAll(0)
    writePortfolioMany(portfolios,-1)
  }

  def writePortfolioStrs(portfolios: List[Portfolio],dayFlag:Int): Unit = {
    //val portfolios: List[Portfolio] = PortfolioMainFunction.getMysqlDataAll(0)
    val portfolioStrs: List[PortfolioStr] = PortfolioMainFunction.getPortfolioFromMysql(portfolios)
    println(portfolioStrs.length)
    val str = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val writer = new PrintWriter(new File("portfolios_" + str + ".txt"))
    for (i <- (0 to portfolioStrs.length - 1)) {
      val sKey = portfolioStrs(i).sKey
      val sValue = portfolioStrs(i).sValue
      val updatetime = portfolioStrs(i).updatetime
      if (i == portfolioStrs.length - 1) {
        writer.write(sKey + "\t" + sValue + "\t" + updatetime)
      } else {
        writer.write(sKey + "\t" + sValue + "\t" + updatetime + "\n")
      }
    }
    writer.flush()
    writer.close()
  }

  def writePortfolioMany(portfolios: List[Portfolio],dayFlag:Int): Unit = {
    val manyFieldPortfolio = PortfolioMainFunction.getManyFieldPortfolio(portfolios)
    val portfolioBeans: List[PortfolioBean] = manyFieldPortfolio.flatMap(_.toSeq)
    val str = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val writer = new PrintWriter(new File("e:\\portfolio_prosecinfo_" + str + ".txt"))
    for (i <- (0 to portfolioBeans.length - 1)) {
      val vBroadcastTime = portfolioBeans(i).getvBroadcastTime().toArray.mkString(",")
      val vStrategyId = portfolioBeans(i).getvStrategyId().toArray.mkString(",")
      println()
      if(i==portfolioBeans.length-1){
        writer.write(portfolioBeans(i).getbRecvAnnounce() + "\t" +
          portfolioBeans(i).getbRecvResearch() + "\t" +
          portfolioBeans(i).getfChipHighPrice() + "\t" +
          portfolioBeans(i).getfChipLowPrice() + "\t" +
          portfolioBeans(i).getfDecreasesPer() + "\t" +
          portfolioBeans(i).getfHighPrice() + "\t" +
          portfolioBeans(i).getfIncreasePer() + "\t" +
          portfolioBeans(i).getfLowPrice() + "\t" +
          portfolioBeans(i).getfMainChipHighPrice() + "\t" +
          portfolioBeans(i).getfMainChipLowPrice() + "\t" +
          portfolioBeans(i).getiCreateTime() + "\t" +
          portfolioBeans(i).getiUpdateTime() + "\t" +
          portfolioBeans(i).getiVersion() + "\t" +
          portfolioBeans(i).getsAiAlert() + "\t" +
          portfolioBeans(i).getsDtSecCode() + "\t" +
          portfolioBeans(i).getsHold() + "\t" +
          portfolioBeans(i).getsKey() + "\t" +
          portfolioBeans(i).getsName() + "\t" +
          portfolioBeans(i).getStCommentInfo_iCreateTime + "\t" +
          portfolioBeans(i).getStCommentInfo_iUpdateTime + "\t" +
          portfolioBeans(i).getStCommentInfo_sComment + "\t" +
          portfolioBeans(i).getUpdateTime + "\t" +
          vBroadcastTime + "\t" +
          vStrategyId)
      }else{
        writer.write(portfolioBeans(i).getbRecvAnnounce() + "\t" +
          portfolioBeans(i).getbRecvResearch() + "\t" +
          portfolioBeans(i).getfChipHighPrice() + "\t" +
          portfolioBeans(i).getfChipLowPrice() + "\t" +
          portfolioBeans(i).getfDecreasesPer() + "\t" +
          portfolioBeans(i).getfHighPrice() + "\t" +
          portfolioBeans(i).getfIncreasePer() + "\t" +
          portfolioBeans(i).getfLowPrice() + "\t" +
          portfolioBeans(i).getfMainChipHighPrice() + "\t" +
          portfolioBeans(i).getfMainChipLowPrice() + "\t" +
          portfolioBeans(i).getiCreateTime() + "\t" +
          portfolioBeans(i).getiUpdateTime() + "\t" +
          portfolioBeans(i).getiVersion() + "\t" +
          portfolioBeans(i).getsAiAlert() + "\t" +
          portfolioBeans(i).getsDtSecCode() + "\t" +
          portfolioBeans(i).getsHold() + "\t" +
          portfolioBeans(i).getsKey() + "\t" +
          portfolioBeans(i).getsName() + "\t" +
          portfolioBeans(i).getStCommentInfo_iCreateTime + "\t" +
          portfolioBeans(i).getStCommentInfo_iUpdateTime + "\t" +
          portfolioBeans(i).getStCommentInfo_sComment + "\t" +
          portfolioBeans(i).getUpdateTime + "\t" +
          vBroadcastTime + "\t" +
          vStrategyId+"\n")
      }
    }
    writer.flush()
    writer.close()
  }

  def writePortfolioGroup(portfolios: List[Portfolio],dayFlag:Int): Unit ={
    val groupInfo = getGroupInfoFromMysql(portfolios)
    val portGroupInfoes = groupInfo.flatMap(_.toSeq)
    val str = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val writer = new PrintWriter(new File("e:\\portfolio_group_" + str + ".txt"))
    for(i <- (0 to portGroupInfoes.length-1)){

      portGroupInfoes(i).getGi_iCreateTime+ "\t" +
      portGroupInfoes(i).getGi_isDel+ "\t" +
      portGroupInfoes(i).getGi_iUpdateTime+ "\t" +
      portGroupInfoes(i).getGi_sGroupName+ "\t" +
      portGroupInfoes(i).getGs_isDel+ "\t" +
      portGroupInfoes(i).getGs_iUpdateTime+ "\t" +
      portGroupInfoes(i).getGs_sDtSecCode+ "\t" +
      portGroupInfoes(i).getiVersion()+ "\t" +
      portGroupInfoes(i).getsKey()+ "\t" +
      portGroupInfoes(i).getUpdateTime

    }
  }

}
