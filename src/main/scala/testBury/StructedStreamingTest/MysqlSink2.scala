package testBury.StructedStreamingTest

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{ForeachWriter, Row}

class MysqlSink2() extends ForeachWriter[Row] {

  var conn: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = DruidUtils.getConnection
    true
  }

  override def process(value: Row): Unit = {
    println("==================================="+value.getAs[String]("name"))
    val statement = conn.prepareStatement("replace into user_test(name) values(?)")
    statement.setString(1,value(0).toString)
    statement.executeUpdate()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
