package tfm.cassandra

import org.apache.spark.sql.SparkSession

object SparkCassandraSession {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("cassandra session")
    .getOrCreate()

  val keySpace: String =  tfm.common.Constants.keySpace
  val tableName: String = tfm.common.Constants.tableNameConstant
}
