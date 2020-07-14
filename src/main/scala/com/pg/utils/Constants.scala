package com.pg.utils

import com.typesafe.config.Config

object Constants {
  //  val ACCESS_KEY = "<ACCESS_KEY>"
  //  val SECRET_ACCESS_KEY = "<SECRET_ACCESS_KEY>"
  //  val S3_BUCKET = "<BUCKET_NAME>"
  val ACCESS_KEY = "AKIAU4WUITJL77JZYNHU"
  val SECRET_ACCESS_KEY = "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"
  val S3_BUCKET = "samarth-test"
  val ERROR = "ERROR"

  def getRedshiftJdbcUrl(redshiftConfig: Config): String = {
    val host = redshiftConfig.getString("host")
    val port = redshiftConfig.getString("port")
    val database = redshiftConfig.getString("database")
    val username = redshiftConfig.getString("username")
    val password = redshiftConfig.getString("password")
    s"jdbc:redshift://${host}:${port}/${database}?user=${username}&password=${password}"
  }

}
