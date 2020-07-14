package com.pg.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.current_date

object Utility {
  def readfromsftp(sparkSession: SparkSession, sftpConfig: Config, filename: String ): DataFrame =  {
      sparkSession.read.
      format("com.springml.spark.sftp")
      .option("host", sftpConfig.getString("hostname"))
      .option("port", sftpConfig.getString("port"))
      .option("username", sftpConfig.getString("username"))
      //        option("password", "Temp1234").
      .option("pem", sftpConfig.getString("pem"))
      .option("fileType", "csv")
      .option("delimiter", "|")
      .load(s"${sftpConfig.getString("directory")}/$filename")

      .withColumn("ins.ts", current_date())
    println("Reading Data")
  }
}