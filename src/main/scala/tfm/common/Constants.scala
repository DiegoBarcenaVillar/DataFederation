package tfm.common

import com.typesafe.config.ConfigFactory

object Constants {

  val tfmConfig = ConfigFactory.parseResources("tfm.conf")

  val applicationName : String = tfmConfig.getString("applicationName")
  val areLocalDataSources : Boolean = tfmConfig.getBoolean("areLocalDataSources")
  val parquetLocalFilePath : String  =  tfmConfig.getString("parquetLocalFilePath")
  val csvFileLocalPath : String  =  tfmConfig.getString("csvLocaFilelPath")
  val parquetFilePath : String  =  tfmConfig.getString("parquetFilePath")
  val csvFilePath : String  =  tfmConfig.getString("csvFilePath")
  val hostPort : String = tfmConfig.getString("hostPort")
  val sessionTimeOut : Integer = tfmConfig.getInt("sessionTimeOut")
  val rootZnodeName : String = tfmConfig.getString("rootZnodeName")
  val receptionPorts : Seq[String] = tfmConfig.getStringList("receptionPorts").toArray.map(t=>t.toString).toSeq
  val columnsLabel : String = tfmConfig.getString("columnsLabel")
  val tableLabelCreateStart : String = tfmConfig.getString("tableLabelCreateStart")
  val tableLabelCreateEnd : String = tfmConfig.getString("tableLabelCreateEnd")
  val tableLabelDrop : String = tfmConfig.getString("tableLabelDrop")
  val keySpace : String = tfmConfig.getString("keySpace")
  val tableNameConstant : String = tfmConfig.getString("tableName")
  val deleteTablesOnStartup : Boolean = tfmConfig.getBoolean("deleteTablesOnStartup")
  val mergingDataFramesFlag : Boolean = tfmConfig.getBoolean("mergingDataFramesFlag")
  val mustZookeeperOn : Boolean = tfmConfig.getBoolean("mustZookeeperOn")
  val joinLabel : String = tfmConfig.getString("joinLabel")
  val tableLabelCreateSql : String = tfmConfig.getString("tableLabelCreateSql")
  val tableLabelDropSql : String = tfmConfig.getString("tableLabelDropSql")
  val getColumnsParquet : String = tfmConfig.getString("getColumnsParquet")
  val getColumnsCsv : String = tfmConfig.getString("getColumnsCsv")
  val getTablesInSpark : String = tfmConfig.getString("getTablesInSpark")
  val getTablesInZookeeper : String = tfmConfig.getString("getTablesInZookeeper")
}