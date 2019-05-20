package testBury.javaTest;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class hiveUtils2{
  public static void main(String[] args) {
    SparkSession sparkSession =
            SparkSession.builder()
                    .appName("Java Spark Hive Example")
                    .master("local[*]")
                    .config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse")
                    .config("fs.hdfs.impl", DistributedFileSystem.class.getName())
                    .config("fs.file.impl", LocalFileSystem.class.getName())
                    .enableHiveSupport()
                    .getOrCreate();

    Dataset<Row> hivedatabases = sparkSession.sql("select * from db_ods.t_pc_web_log_dt limit 100 ");
    hivedatabases.show();
    sparkSession.sql("use pdw");
    sparkSession.sql("show tables");
  }
  }