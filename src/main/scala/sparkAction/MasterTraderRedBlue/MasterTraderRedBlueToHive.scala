package sparkAction.MasterTraderRedBlue

import com.niufu.tar.bec.BullBearTrendIndicatorCache
import com.qq.tars.protocol.tars.BaseDecodeStream
import conf.ConfigurationManager
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaUtil.{RandomCharData, StructUtil}
import sparkAction.UllBearTrendCache
import scala.collection.{JavaConverters, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * ClassName NfRiskAssessmentUserCommitRecordToHive
  * Description TODO
  * Author lenovo
  * Date 2019/7/19 14:41
  **/

object MasterTraderRedBlueToHive {
  private val TABLERESULT: String = ConfigurationManager.getProperty("masterTraderRedBlueTable")
  def insertToHive(ullBearTrendCacheData: Dataset[UllBearTrendCache], spark: SparkSession) = {
    import spark.implicits._
    val allData = ullBearTrendCacheData.map(line => {
      val flagNull = null;
      val data_key = line.data_key
      val update_time = line.update_time
      val data_value = line.data_value
      val byteArray = RandomCharData.hexStringToByteArray(data_value)
      val stream = new BaseDecodeStream(byteArray)
      val bullBearTrendIndicatorCache = new BullBearTrendIndicatorCache()
      bullBearTrendIndicatorCache.readFrom(stream)
      val sSecCode = bullBearTrendIndicatorCache.getSSecCode
      val sSecName = bullBearTrendIndicatorCache.getSSecName
      val iIndicatorType = bullBearTrendIndicatorCache.getIIndicatorType
      val vIndicatorParam = bullBearTrendIndicatorCache.getVIndicatorParam
      val alls = new ArrayBuffer[UllBearTrendCacheAll]()
      if (null != vIndicatorParam && !vIndicatorParam.isEmpty) {
        for (i <- (0 until (vIndicatorParam.size()))) {
          val param = vIndicatorParam.get(i)
          val iDate = param.getIDate
          val mParam = param.getMParam
          var scalaMap: mutable.Map[java.lang.String, java.lang.Double] = null
          if (mParam != null) {
            scalaMap = JavaConverters.mapAsScalaMapConverter(mParam).asScala
          }
          val ullBearTrendCacheAll = UllBearTrendCacheAll(data_key, update_time, sSecCode, sSecName, iIndicatorType, iDate, scalaMap)
          alls.+=(ullBearTrendCacheAll)
        }
      } else {
        val ullBearTrendCacheAll = UllBearTrendCacheAll(data_key, update_time, sSecCode, sSecName, iIndicatorType, flagNull, flagNull)
        alls.+=(ullBearTrendCacheAll)
      }
      alls
    })
    val oneData = allData.flatMap(_.toSeq)
    oneData.repartition(1).createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite table ${TABLERESULT} select * from tempTable")
  }
}

case class UllBearTrendCacheAll(data_key: String, update_time: String, sSecCode: String, sSecName: String, iIndicatorType: Integer, iDate: Integer, scalaMap: mutable.Map[java.lang.String, java.lang.Double])