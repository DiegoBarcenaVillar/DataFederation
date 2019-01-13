package tfm.cassandra

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnectorConf
import tfm.common.Constants

import SparkCassandraSession._

object CassandraFeeder {

  var fileDF  : DataFrame = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .load(Constants.csvFileLocalPath)

  def main(args: Array[String]): Unit = {

    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

    spark.setCassandraConf("Test Cluster", CassandraConnectorConf.ConnectionHostParam.option("localhost")
        ++ CassandraConnectorConf.ConnectionPortParam.option(9042))

    val connector : CassandraConnector = CassandraConnector.apply(spark.sparkContext.getConf)

    val session : Session  = connector.openSession

    val createKeySpace : String = """CREATE KEYSPACE """.concat(keySpace).concat(
      """ WITH REPLICATION = { 'class' : 'SimpleStrategy'
                              , 'replication_factor' : 1}""")
    import com.datastax.driver.core.exceptions.AlreadyExistsException
    import com.datastax.spark.connector._

    try {
      val createKeySpaceResultset = session.execute(createKeySpace)
      fileDF.createCassandraTable(keySpace, tableName)
      fileDF.write.cassandraFormat(tableName, keySpace).save()
    }catch {
      case e: AlreadyExistsException =>
    }
  }
}

