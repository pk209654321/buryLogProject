package sparkAction.polarLightForAction

import conf.ConfigurationManager

/**
  * ClassName PolarLigthSql
  * Description TODO
  * Author lenovo
  * Date 2019/5/17 11:17
  **/
object PolarLigthSql {
  private val CHANNEL: String = ConfigurationManager.getProperty("jiguang.channel")
  //极光sql
  val hql=
    s"""
      |select p1.hp_stat_date
      |			        ,p1.guid
      |					,p1.device_id
      |					,p1.device_type
      |					,p1.conv_type
      |					,p1.access_time
      |			   from (select p1.hp_stat_date
      |					        ,p1.guid
      |							,p1.device_id
      |							,p1.device_type
      |							,p1.conv_type
      |							,p1.access_time
      |							,row_number()over(partition by device_id order by p1.access_time asc) as num
      |					   from (select p1.hp_stat_date
      |									,p1.guid
      |									,case when p1.download_channel = 'appstore' then idfa else imei end as device_id
      |									,case when p1.download_channel = 'appstore' then 'iOS' else 'Android' end as device_type
      |									,'APP_ACTIVE' as conv_type
      |									,p1.access_time
      |							  from  (select p1.guid
      |											,p1.hp_stat_date
      |											,p1.download_channel
      |											,p1.access_time
      |											,p1.imei
      |											,p1.idfa
      |									   from	(select guid
      |												   ,hp_stat_date
      |												   ,download_channel
      |												   ,access_time
      |												   ,imei
      |												   ,idfa
      |												   ,row_number()over(partition by guid order by access_time asc) as num
      |											  from db_ods.t_stock_login_log_rt
      |											 where hp_stat_date >= date_sub(current_date, 2)
      |											   and access_time <> ''
      |											) p1
      |									  where p1.num = 1
      |									) p1
      |							   left join
      |									(select guid
      |											,min(first_login_date) as first_date
      |									   from pdw.dim_guid_user_info
      |									  group by guid
      |									) p2
      |								 on p1.guid = p2.guid
      |							  where (p2.guid is null or p2.first_date = p1.hp_stat_date)
      |								and download_channel in ('${CHANNEL}', 'appstore')
      |								and (imei <> '' or idfa <> '')
      |							) p1
      |					) p1
      |			  where p1.num = 1
      |
      """.stripMargin
}
