package com.pg

import com.pg.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object DataLoading {
    def main(args: Array[String]) : Unit = {
      try {
        val sparkSession = SparkSession.builder
          .master("local[*]")
          .appName("Practice Project").getOrCreate()

        sparkSession.sparkContext.setLogLevel(Constants.ERROR)
        val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")

        val s3Config = rootConfig.getConfig("s3_conf")
        val filepath = s"s3n://${s3Config.getString("s3_bucket")}/sftpwrite"
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))



        val sftpConfig = rootConfig.getConfig("sftp_conf")

        val olTxndf = Utility.readfromsftp()
        olTxnDf.show()

        olTxnDf.write
            .partitionBy("ins.ts")
          .mode("overwrite")
          .option("header", "true")
          .parquet(filepath)

        sparkSession.close()
      } catch {
        case ex: Throwable => {
          ex.printStackTrace()
        }
      }
    }


}
