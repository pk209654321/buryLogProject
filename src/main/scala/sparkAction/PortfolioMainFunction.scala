package sparkAction

import bean.{PortGroupInfo, PortfolioBean}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.{ProSecInfo, ProSecInfoList}
import com.qq.tars.protocol.tars.BaseDecodeStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.portfolioHive.PortfolioProSecInfoHiveInsertObject

import scala.collection.mutable

/**
  * ClassName PortfolioMainFunction
  * Description TODO
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object PortfolioMainFunction {
  def main(args: Array[String]): Unit = {
    try {
      val dataMysql = getMysqlData()
     /* for (elem <- dataMysql) {
        val key = elem.sKey
        val i = key.indexOf(":")
        val str = key.substring(i+1)
        elem.sKey=str
      }*/
      val portfolioStrs = getPortfolioFromMysql(dataMysql)
      val manyFieldPortfolio = getManyFieldPortfolio(dataMysql)
      val groupInfo = getGroupInfoFromMysql(dataMysql)
      //自选股保存的证券信息
      val portfolioBeans: List[PortfolioBean] = manyFieldPortfolio.flatMap(_.toSeq)
      //用户自选股组信息
      val portGroupInfoes = groupInfo.flatMap(_.toSeq)
      val diffDay: Int = args(0).toInt
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("PortfolioMainFunction")
      sparkConf.set("spark.akka.frameSize","512")
      //sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      //sparkConf.registerKryoClasses(Array(classOf[Portfolio],classOf[PortfolioStr],classOf[PortfolioBean]))//对Counter类进行注册
      if (local) {
        System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      val hc: HiveContext = new HiveContext(sc)
      val portfolias = sc.parallelize(portfolioStrs ,1)
      val many = sc.parallelize(portfolioBeans,1)
      val groups = sc.parallelize(portGroupInfoes,1)
      PortfolioProSecInfoHiveInsertObject.insertPortfolioToHive(portfolias,hc,diffDay)
      PortfolioProSecInfoHiveInsertObject.insertPortfolioManyToHive(many,hc,diffDay)
      PortfolioProSecInfoHiveInsertObject.insertPortfolioToHiveGroupInfo(groups,hc,diffDay)
      sc.stop()
    } catch {
      case e: Throwable => e.printStackTrace();MailUtil.sendMail("spark用户自选股解析入库", "失败")
    }
  }

  //拉取masql 中的数据

  def getMysqlData()={
    DBs.setupAll()
    val peoples = NamedDB('mysql).readOnly { implicit session =>
      SQL("select * from t_portfolio").map(rs => Portfolio(rs.string("sKey"),rs.bytes("sValue"),rs.string("updatetime"))).list().apply()
    }
    peoples
  }

  def getGroupInfoFromMysql(dataMysql: scala.List[Portfolio])={
    dataMysql.map(one=> {
      val key = one.sKey
      val value = one.sValue
      val updatetime = one.updatetime
      val stream = new BaseDecodeStream(value)
      val list = new ProSecInfoList()
      list.readFrom(stream)
      val iVersion = list.iVersion
      val groupInfo = list.getVGroupInfo
      val array = new mutable.ArrayBuffer[PortGroupInfo]()
      for (i <- 0 until(groupInfo.size())){

        val gi = groupInfo.get(i)
        val gi_iCreateTime = gi.getICreateTime
        val gi_iUpdateTime = gi.getIUpdateTime
        val gi_sGroupName = gi.getSGroupName
        val gi_del = gi.isDel
        val gsList = gi.getVGroupSecInfo
        for (j <- 0 until(gsList.size())){
          val portGroupInfo = new PortGroupInfo
          val gsInfo = gsList.get(j)
          val gs_del = gsInfo.isDel
          val gs_iUpdateTime = gsInfo.getIUpdateTime
          val gs_sDtSecCode = gsInfo.getSDtSecCode
          portGroupInfo.setGi_iCreateTime(gi_iCreateTime)
          portGroupInfo.setGi_isDel(gi_del)
          portGroupInfo.setGi_iUpdateTime(gi_iUpdateTime)
          portGroupInfo.setGi_sGroupName(gi_sGroupName)
          portGroupInfo.setGs_isDel(gs_del)
          portGroupInfo.setGs_iUpdateTime(gs_iUpdateTime)
          portGroupInfo.setGs_sDtSecCode(gs_sDtSecCode)
          portGroupInfo.setiVersion(iVersion)
          portGroupInfo.setsKey(key)
          portGroupInfo.setUpdateTime(updatetime)
          array.+=(portGroupInfo)
        }
      }
      array
    })
  }
  def getPortfolioFromMysql(dataMysql: scala.List[Portfolio]):List[PortfolioStr] ={
    val portfolioStrs = dataMysql.map(one => {
      val value = one.sValue
      val stream = new BaseDecodeStream(value)
      val list = new ProSecInfoList()
      list.readFrom(stream)
      val sValue = JSON.toJSONString(list, SerializerFeature.WriteMapNullValue)
      PortfolioStr(one.sKey, sValue, one.updatetime)
    })
    portfolioStrs
  }

  def getManyFieldPortfolio(dataMysql: scala.List[Portfolio])={
    val listTup = dataMysql.map(one => {
      val value = one.sValue
      val stream = new BaseDecodeStream(value)
      val list = new ProSecInfoList()
      list.readFrom(stream)
      val iVersion = list.iVersion
      val vProSecInfo = list.getVProSecInfo
      val array = new mutable.ArrayBuffer[PortfolioBean]()
      for (i <- 0 until (vProSecInfo.size())) {
        val portfolioBean = new PortfolioBean
        val info = vProSecInfo.get(i)
        val iCreateTime = info.getStCommentInfo.getICreateTime
        val iUpdateTime = info.getStCommentInfo.getIUpdateTime
        var sComment = info.getStCommentInfo.getSComment
        if(sComment==""){
          sComment=null
        }
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
        portfolioBean.setiVersion(iVersion)
        portfolioBean.setsAiAlert(info.isAiAlert)
        portfolioBean.setsDel(info.isDel)
        portfolioBean.setsDKAlert(info.isDKAlert)
        portfolioBean.setsDtSecCode(info.getSDtSecCode)
        portfolioBean.setsHold(info.isHold)
        portfolioBean.setsName(info.getSName)
        portfolioBean.setStCommentInfo_iCreateTime(iCreateTime)
        portfolioBean.setStCommentInfo_iUpdateTime(iUpdateTime)
        portfolioBean.setStCommentInfo_sComment(sComment)
        portfolioBean.setsKey(one.sKey)
        portfolioBean.setUpdateTime(one.updatetime)
        portfolioBean.setvBroadcastTime(info.getVBroadcastTime)
        portfolioBean.setvStrategyId(info.getVStrategyId)
        array.+=(portfolioBean)
      }
      array
    })
    listTup
  }
}

case class Portfolio (var sKey:String,var sValue:Array[Byte],var updatetime:String)
case class PortfolioStr(var sKey:String,var sValue:String,var updatetime:String)
