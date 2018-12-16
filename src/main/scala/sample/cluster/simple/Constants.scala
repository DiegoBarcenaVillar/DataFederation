package sample.cluster.simple

class Constants {

  val applicationName : String = "datafederation"
  val parquetFilePath : String  =  "C:\\Users\\diegobarcena\\Documents\\U-tad\\TFM\\Datos_II\\airlines_parquet\\"
  val hostPort : String = "127.0.0.1:2181"
  val sessionTimeOut : Integer = 600000
  val rootZnodeName : String = "/tables"
  val receptionPorts = Seq("5000", "2551", "2552")
  val columnsLabel = "COLUMNS:"
  val tableLabelCreate = "AND NAME:"
  val tableLabelDrop = "DROP TABLE WITH NAME:"
}
