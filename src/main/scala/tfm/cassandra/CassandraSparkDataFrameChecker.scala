package tfm.cassandra

import SparkCassandraSession._

object CassandraSparkDataFrameChecker {

  val spark = SparkCassandraSession.spark

  def main(args: Array[String]): Unit = {

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, "keyspace" -> keySpace))
      .load()

    System.out.println(df.show(10))
  }
}
