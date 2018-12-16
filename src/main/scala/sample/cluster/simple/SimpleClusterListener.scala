package sample.cluster.simple

import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.ActorLogging
import akka.actor.Actor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import spray.json._
import DefaultJsonProtocol._
import misc.JSON.spark
import sample.cluster.simple.AllowedActions.AllowedActions

import scala.collection.mutable

class SimpleClusterListener(port : String) extends Actor with ActorLogging {

  val mustZookeeperOn = true

  val runningPort = port

  val constants : Constants = new Constants()

  val cluster : Cluster = Cluster(context.system)

  val sparkSession : SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName(constants.applicationName)
    .getOrCreate()

  var parquetFileDF  : DataFrame = sparkSession
    .read.load(constants.parquetFilePath)

  val columns = parquetFileDF.dtypes

  var zookeeperListener : Executor = null
  var zookeeperModifier : ZookeeperModifier = null
  var mapOfCreatedStrings : mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])

    System.out.println(parquetFileDF.show(10) + this.runningPort)

    if (mustZookeeperOn) {
        zookeeperListener = new Executor(constants.hostPort, constants.rootZnodeName
          , constants.sessionTimeOut, runningPort, this)
        new Thread(zookeeperListener).start

        zookeeperModifier = new ZookeeperModifier(constants.hostPort, constants.rootZnodeName
          , constants.sessionTimeOut, runningPort, this)
        zookeeperModifier.startUp()
        zookeeperModifier.read()
    }
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {

    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}"
                                      , member.address, previousStatus)
    case msg : String =>
      System.out.println("Message Received: " + msg)
      sender ! processString(msg)
    case _ =>
  }

  def processString (sqlText : String ) :  Object = {
    try {
      print("SimpleClusterListener processString", runningPort, "beginning")

      if (sqlText.startsWith("GET COLUMNS")) {
        return getColumns(sqlText)
      } else if (sqlText.startsWith("CREATE TABLE")) {
        createTable(sqlText)
        return Array("Done").toJson
      } else if (sqlText.startsWith("DROP TABLE")) {
        dropTable(sqlText)
        return Array("Done").toJson
      }else if (sqlText.startsWith("SELECT")) {
        return querySentence(sqlText)
      }else if (sqlText.startsWith("GET TABLES")) {
        listTables()
        return Array("Done").toJson
      } else{
        return Array("The Requested Operation is not Available in the System").toJson
      }
    }catch {
      case ex: org.apache.spark.sql.catalyst.parser.ParseException => {
        ex.printStackTrace()
        null
      }
    }finally{
      print("SimpleClusterListener processString", runningPort, "end")
    }
  }

  def createTable(createString : String): Unit = {
    print("SimpleClusterListener createTable", runningPort, "beginning")
    val tableName = getTableName(createString, AllowedActions.Create)

    updateZookeeper(createString, tableName, AllowedActions.Create)
    updateSpark(createString, AllowedActions.Create)
    print("SimpleClusterListener createTable", runningPort, "end")
  }

  def dropTable(createString : String): Unit = {
    print("SimpleClusterListener dropTable", runningPort, "beginning")
    val tableName = getTableName(createString, AllowedActions.Drop)

    updateZookeeper(createString, tableName, AllowedActions.Drop)
    updateSpark(createString, AllowedActions.Drop)
    print("SimpleClusterListener dropTable", runningPort, "end")
  }

  def updateZookeeper(createString : String, tableName : String, action : AllowedActions): Unit = {
    print("SimpleClusterListener updateZookeeper", runningPort, "beginning")
    if (action.equals(AllowedActions.Create)) {

      System.out.println("Create String: " + createString + " => Creating")

      mapOfCreatedStrings.put(tableName, createString)
      zookeeperModifier.createTable(tableName, createString)
      zookeeperModifier.read()

      mapOfCreatedStrings.toStream.foreach(t => System.out.println(t._1 + " " + t._2 + " " + this.runningPort))
    }else if (action.equals(AllowedActions.Drop)){

      System.out.println("Create String: " + createString + " => Dropping")

      mapOfCreatedStrings.remove(tableName)
      zookeeperModifier.dropTable(tableName)
      zookeeperModifier.read()

      mapOfCreatedStrings.toStream.foreach(t => System.out.println(t._1 + " " + t._2 + " " + this.runningPort))
    }
    print("SimpleClusterListener updateZookeeper", runningPort, "end")
  }

  def updateSpark(clientRequest : String, action : AllowedActions): Unit = {
    print("SimpleClusterListener updateSpark", runningPort, "beginning")
    try {
      if (action.equals(AllowedActions.Create)) {

        val tableName = getTableName(clientRequest, AllowedActions.Create)

        if (!(mapOfCreatedStrings.contains(tableName) && mapOfCreatedStrings.get(tableName).equals(clientRequest))) {

          mapOfCreatedStrings.put(tableName, clientRequest)

          val columnNames = getColumnNames(clientRequest)

          val newDF = parquetFileDF.selectExpr(columnNames: _*)

          newDF.createOrReplaceTempView(tableName)
        }
      } else if (action.equals(AllowedActions.Drop)) {

        val tableName = getTableName(clientRequest, AllowedActions.Drop)

        if (mapOfCreatedStrings.contains(tableName)) {
          mapOfCreatedStrings.remove(tableName)
          this.sparkSession.catalog.dropTempView(tableName)
        }
      }
    }finally {
      listTables()
      print("SimpleClusterListener updateSpark", runningPort, "end")
    }
  }

  def listTables() : Unit = {
    print("SimpleClusterListener listTables", runningPort, "beginning")
    val tableArray = this.sparkSession.catalog.listTables().collect()

    print("SimpleClusterListener listTables", runningPort, "Number of tables in Spark Session "
      .concat(Integer.toString(tableArray.length)))

    this.sparkSession.catalog.listTables().collect().toStream.foreach(t=> System.out.println("Table in Spark Session "
      .concat(t.name.concat(" ").concat(runningPort))))

    print("SimpleClusterListener listTables", runningPort, "end")

  }

  def getTableName(clientRequest : String, action : AllowedActions) : String = {
    print("SimpleClusterListener getTableName", runningPort, "beginning")
    var tableNameString : String = ""

    if(action.equals(AllowedActions.Create)) {
      val tableLabel = constants.tableLabelCreate
      val finalIndex = clientRequest.toUpperCase.indexOf(tableLabel)
      tableNameString = clientRequest.substring(finalIndex + tableLabel.length).trim
    }else if(action.equals(AllowedActions.Drop)){
      val tableLabel = constants.tableLabelDrop
      val finalIndex = clientRequest.toUpperCase.indexOf(tableLabel)
      tableNameString = clientRequest.substring(finalIndex + tableLabel.length).trim
    }
    print("SimpleClusterListener getTableName", runningPort, "end")
    tableNameString
  }

  def getColumnNames(clientRequest : String) : Array[String] = {
    print("SimpleClusterListener getColumnNames", runningPort, "beginning")
    val columnsLabel = constants.columnsLabel
    val tableLabel = constants.tableLabelCreate

    val initialIndex = clientRequest.toUpperCase.indexOf(columnsLabel)
    val finalIndex = clientRequest.toUpperCase.indexOf(tableLabel)

    val columnNamesString = clientRequest.substring(initialIndex + columnsLabel.length , finalIndex).trim
    val columnNames = columnNamesString.split(",").map(t=> t.trim)
    print("SimpleClusterListener getColumnNames", runningPort, "end")
    columnNames
  }

  def querySentence(clientRequest : String): JsValue = {
    try {
      this.sparkSession.sql(clientRequest).toJSON.collect().toJson
    }catch{
      case ex: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
        return Array(ex.getMessage()).toJson
      case ex: java.lang.Exception =>
        return Array(ex.getMessage()).toJson
    }
  }

  def getColumns(clientRequest : String): JsValue = {
    this.columns.toJson
  }

//  def dummyQuerySentence(clientRequest : String): JsValue = {
//
//    var parquetFileDF = spark.read.load("C:\\Users\\diegobarcena\\Documents\\U-tad\\TFM\\Datos_II\\airlines_parquet\\")
//
//    System.out.println(parquetFileDF.show(10) + this.runningPort)
//
//    val year = parquetFileDF.select("year", "flight_num")
//
//    year.createOrReplaceTempView("years")
//
//    val years = spark.sql("SELECT * FROM years LIMIT 2")
//
//    System.out.println(years.show + this.runningPort)
//
//    val arrayStr = years.toJSON.collect().toJson
//
//    arrayStr
//
//  }

  def print(method: String, port: String, trace: String): Unit = {
    System.out.println("#############" + method + " " + port + " " + trace + "#############")
  }
}