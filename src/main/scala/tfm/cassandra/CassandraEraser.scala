package tfm.cassandra

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}

import SparkCassandraSession._

import org.apache.spark.sql.cassandra._

object CassandraEraser {

  def main(args: Array[String]): Unit = {

    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

    spark.setCassandraConf("Test Cluster", CassandraConnectorConf.ConnectionHostParam.option("localhost")
      ++ CassandraConnectorConf.ConnectionPortParam.option(9042))

    val connector : CassandraConnector = CassandraConnector.apply(spark.sparkContext.getConf)
    val session : Session  = connector.openSession

    val dropKeySpace = """DROP KEYSPACE """.concat(keySpace)
    val dropTable = """DROP TABLE IF EXISTS """.concat(keySpace).concat(""".""").concat(tableName)

    try {
      val dropTableResultset = session.execute(dropTable) // Creates Catalog Entry registering an existing Cassandra Table
    }
    catch{
      case _=>
    }

    try {
      val dropKeySpaceResultset = session.execute(dropKeySpace)
    }
    catch{
      case _=>
    }

    val verificationResultset = spark.sql("SELECT * FROM ".concat(keySpace).concat(".").concat(tableName)).show
  }
}
