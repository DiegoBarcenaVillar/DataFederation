package tfm.cluster

import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.ActorLogging
import akka.actor.Actor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json._
import DefaultJsonProtocol._

import scala.collection.mutable
import tfm.common.Constants._
import AllowedActions.AllowedActions
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import tfm.cluster.exception.{JoinException, NotExistingColumnException}

class ClusterListener(port : String) extends Actor with ActorLogging {

  //val mustZookeeperOn : Boolean = mustZookeeperOn
  val runningPort : String = port

  val cluster : Cluster = Cluster(context.system)

  val sparkSession : SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName(applicationName)
    .config("spark.sql.inMemoryColumnarStorage.compressed", value = true)
    .getOrCreate()

  var parquetFileDF  : DataFrame = null.asInstanceOf[DataFrame]
  var csvFileDF  : DataFrame = null.asInstanceOf[DataFrame]

  if(areLocalDataSources){
    parquetFileDF = sparkSession
      .read.load(parquetLocalFilePath)

    csvFileDF = sparkSession.read.format("csv")
      .option("header", "true").load(csvFileLocalPath)
  }else{
    parquetFileDF = sparkSession
      .read.load(parquetFilePath)

    csvFileDF = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableNameConstant, "keyspace" -> keySpace))
      .load()
  }

  var zookeeperListener : ZookeeperListener = null.asInstanceOf[ZookeeperListener]
  var zookeeperModifier : ZookeeperModifier = null.asInstanceOf[ZookeeperModifier]
  var mapOfCreatedStrings : mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])

    System.out.println(parquetFileDF.show(10) + this.runningPort)
    System.out.println("")
    System.out.println("")
    System.out.println(csvFileDF.show(10) + this.runningPort)

    startingUpDataFrames()
    changeColumnNames()

    if(mergingDataFramesFlag)
      joiningDataFrames()

    System.out.println(parquetFileDF.where("origin='JFK'").show(10) + this.runningPort + " MODIFIED")

    if (mustZookeeperOn) {
        zookeeperListener = new ZookeeperListener(/*hostPort,*/ rootZnodeName
          /*, sessionTimeOut*/, runningPort, this)

        new Thread(zookeeperListener).start()

        zookeeperModifier = new ZookeeperModifier(hostPort, rootZnodeName
          , sessionTimeOut, runningPort, this)
        zookeeperModifier.startUp()
        zookeeperModifier.read()
    }
  }
  override def postStop(): Unit = cluster.unsubscribe(self)
  def receive : PartialFunction[Any, Unit] = {

    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}"
                                      , member.address, previousStatus)
    case msg : String =>
      System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      System.out.println("$$$$$$$$$$$$$$$$$$$ Message Received: " + msg + " $$$$$$$$$$$$$$$$$$$")
      System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      sender ! processString(msg)
    case _ =>
  }
  def startingUpDataFrames(): Unit ={

    getColumns.foreach(t=>{
        if(t._2.equalsIgnoreCase("BinaryType")){
          this.parquetFileDF = this.parquetFileDF.withColumn(t._1, parquetFileDF(t._1).cast(DataTypes.StringType))
        }
      })
  }
  def joiningDataFrames(): Unit ={

    sparkSession.conf.set("spark.sql.crossJoin.enabled", value = true)

    var newColumNamesOrigin : mutable.Seq[String] = mutable.Seq.empty[String]
    var newColumNamesDest : mutable.Seq[String] = mutable.Seq.empty[String]

    this.csvFileDF.dtypes.foreach(t=>{
        newColumNamesOrigin = newColumNamesOrigin :+ t._1.concat("_origin")
        newColumNamesDest = newColumNamesDest :+ t._1.concat("_dest")
      }
    )

    val csvFileOriginDF : DataFrame =  this.csvFileDF.toDF(newColumNamesOrigin : _*)
    val csvFileDestDF : DataFrame =  this.csvFileDF.toDF(newColumNamesDest : _*)

    this.parquetFileDF = this.parquetFileDF.join(csvFileOriginDF).where(parquetFileDF("origin")===csvFileOriginDF("iata_origin"))
    this.parquetFileDF = this.parquetFileDF.join(csvFileDestDF).where(parquetFileDF("dest")===csvFileDestDF("iata_dest"))
  }
  def joiningDataFrames(joinColumnNames : Array[String]): DataFrame ={

    var newFileDF : DataFrame = null.asInstanceOf[DataFrame]

    sparkSession.conf.set("spark.sql.crossJoin.enabled", value = true)

    newFileDF = parquetFileDF.join(csvFileDF).where(parquetFileDF(joinColumnNames(0))===csvFileDF(joinColumnNames(1)))
    newFileDF
  }
  def joiningDataFrames(joinColumnNames : Array[String], strAppender : String ): DataFrame ={

    var newFileDF : DataFrame = null.asInstanceOf[DataFrame]

    val newSecondColumnName = joinColumnNames(1).concat(strAppender)

    sparkSession.conf.set("spark.sql.crossJoin.enabled", value = true)

    var newColumnNames : mutable.Seq[String] = mutable.Seq.empty[String]
    this.csvFileDF.dtypes.foreach(t=>{
            newColumnNames = newColumnNames :+ t._1.concat(strAppender)
          }
    )

    val csvFileDF : DataFrame =  this.csvFileDF.toDF(newColumnNames : _*)

    newFileDF = this.parquetFileDF.join(csvFileDF).where(parquetFileDF(joinColumnNames(0))===csvFileDF(newSecondColumnName))
    newFileDF = parquetFileDF.join(csvFileDF).where(parquetFileDF(joinColumnNames(0))===csvFileDF(joinColumnNames(1)))
    newFileDF
  }
  def changeColumnNames() : Unit = {

    val columnNamesRepeated: Map[String, String] = anyColumnNameRepeated

    if (columnNamesRepeated.nonEmpty) {
      var newColumnNames: mutable.Seq[String] = mutable.Seq.empty[String]
      this.csvFileDF.dtypes.foreach(t => {
        if (columnNamesRepeated.contains(t._1))
          newColumnNames = newColumnNames :+ t._1.concat("_")
        else
          newColumnNames = newColumnNames :+ t._1
      })

      this.csvFileDF = this.csvFileDF.toDF(newColumnNames: _*)
    }
  }
  def anyColumnNameRepeated() :  Map[String,String] = {

    val sameNameColumns : Map[String,String] = this.parquetFileDF.dtypes.toMap.filter(t=> this.csvFileDF.dtypes.toMap.contains(t._1))

    sameNameColumns
  }
  def processString (sqlTextReceived : String ) :  JsValue = {
    var jsReturning : JsValue =  "Done".toJson
    try {
      print("SimpleClusterListener processString", runningPort, "beginning")

      val sqlText : String = formatRequest(sqlTextReceived)

      if (sqlText.startsWith(getColumnsParquet)) {
        getColumnsParquetJson

      }else if (sqlText.startsWith(getColumnsCsv)) {
        getColumnsCsvJson

      }else if (sqlText.startsWith(getTablesInSpark)) {
        listTablesSpark()
      }else if (sqlText.startsWith(getTablesInZookeeper)) {
        listTablesZookeeper()
      }else if (sqlText.startsWith(tableLabelCreateStart)) {
        createTable(sqlText, AllowedActions.Create)

      }else if (sqlText.startsWith(tableLabelCreateSql)) {
        createTable(sqlText, AllowedActions.CreateSql)

      }else if (sqlText.startsWith(tableLabelDrop)) {
        dropTable(sqlText, AllowedActions.Drop)
        "Done".toJson

      }else if (sqlText.startsWith(tableLabelDropSql)) {
        dropTable(sqlText, AllowedActions.DropSql)
        "Done".toJson

      }/*else if (sqlText.startsWith("SELECT")) {
        querySentenceArray(sqlText)
      }*/else{
        querySentenceArray(sqlText)
        //"The Requested Operation is not Available in the System".toJson
      }
    }catch {
      case e: NotExistingColumnException =>
        e.getMessage.toJson
      case e: JoinException =>
        e.getMessage.toJson
      case e: NoSuchTableException =>
        e.getMessage.toJson
      case e: Exception =>
        e.printStackTrace()
        e.getMessage.toJson
    }
  }
  def formatRequest(sqlText : String): String = {

    val sqlRequest : String = sqlText.split("\\s+").reduce((x,y)=> x.concat(" ").concat(y))
    sqlRequest
  }

  @throws(classOf[NotExistingColumnException])
  @throws(classOf[JoinException])
  @throws(classOf[NoSuchTableException])
  @throws(classOf[Exception])
  def createTable(createString : String, action : AllowedActions): JsValue = {
    print("SimpleClusterListener createTable", runningPort, "beginning")

    var jsReturned : JsValue = "Done".toJson

    try{
      if(updateSpark(createString, action))
        updateZookeeper(createString, action)

      jsReturned
    }catch{
      case e : NotExistingColumnException =>
        throw e
      case e : JoinException =>
        throw e
      case e : NoSuchTableException =>
        throw e
      case e : Exception =>
        throw e
    }
  }

  @throws(classOf[NotExistingColumnException])
  @throws(classOf[JoinException])
  @throws(classOf[NoSuchTableException])
  @throws(classOf[Exception])
  def dropTable(createString : String, action : AllowedActions): Unit = {
    try{
      print("SimpleClusterListener dropTable", runningPort, "beginning")

      if(updateSpark(createString, action))
        updateZookeeper(createString, action)

      print("SimpleClusterListener dropTable", runningPort, "end")
    }catch{
      case e : NotExistingColumnException =>
        throw e
      case e : JoinException =>
        throw e
      case e : NoSuchTableException =>
        throw e
      case e : Exception =>
        throw e
    }
  }

  @throws(classOf[NotExistingColumnException])
  @throws(classOf[JoinException])
  @throws(classOf[NoSuchTableException])
  @throws(classOf[Exception])
  def updateSpark(clientRequest : String, action : AllowedActions): Boolean = {
    print("SimpleClusterListener updateSpark", runningPort, "beginning")

    var returnBoolean : Boolean = false
    val tableNameStr = getTableName(clientRequest, action)

    try {
      if (action.equals(AllowedActions.Create)) {

        if (!(mapOfCreatedStrings.contains(tableNameStr) && mapOfCreatedStrings(tableNameStr).equals(clientRequest))) {

          mapOfCreatedStrings.put(tableNameStr, clientRequest)

          val columnNames = getColumnNames(clientRequest)

          var scenario: Int = null.asInstanceOf[Int]

          try {
            scenario = createTableScenario(columnNames)
          } catch {
            case e: NotExistingColumnException =>
              throw e
          }

          var joinColumnNames = null.asInstanceOf[Array[String]]

          if (scenario == 13) {
            if (clientRequest.indexOf("JOIN") != -1) {
              joinColumnNames = getJoinColumnNames(clientRequest)
                .getOrElse(throw JoinException("Join Section: First Column Must Be Parquet One, Second Column Must Be Csv One"))
            } else {
              throw JoinException("Columns From Different DataSources Without Join Section in Request String")
            }
          } else if (scenario == 23) {
            throw JoinException("Columns From Different DataSources With Same Name")
          }

          var newDF: DataFrame = createNewTableDF(columnNames, scenario, joinColumnNames)

          newDF.createOrReplaceTempView(tableNameStr)
          returnBoolean = true
        }
      } else if (action.equals(AllowedActions.CreateSql)) {

        if (!(mapOfCreatedStrings.contains(tableNameStr) && mapOfCreatedStrings(tableNameStr).equals(clientRequest))) {
          mapOfCreatedStrings.put(tableNameStr, clientRequest)
          try {
            querySentenceArray(clientRequest)
            returnBoolean = true
          } catch {
            case e: NoSuchTableException =>
              throw e
            case e: Exception =>
              throw e
          }
        }
      } else if (action.equals(AllowedActions.Drop)) {

        if (mapOfCreatedStrings.contains(tableNameStr)) {
          mapOfCreatedStrings.remove(tableNameStr)
        }

        returnBoolean = this.sparkSession.catalog.dropTempView(tableNameStr)

      } else if (action.equals(AllowedActions.DropSql)){

        if (mapOfCreatedStrings.contains(tableNameStr)) {
          mapOfCreatedStrings.remove(tableNameStr)
        }

        try {
          querySentenceArray(clientRequest)
          returnBoolean = true
        }catch{
          case e: NoSuchTableException =>
            throw e
          case e: Exception =>
            throw e
        }
      }
      returnBoolean
    }finally {
      listTablesSpark()
      listTablesZookeeper()
      print("SimpleClusterListener updateSpark", runningPort, "end")
      returnBoolean
    }
  }

  def updateZookeeper(createString : String, action : AllowedActions): Unit = {
    print("SimpleClusterListener updateZookeeper", runningPort, "beginning")

    val tableNameStr= getTableName(createString, action)

    if (action.equals(AllowedActions.Create)) {
        System.out.println("Create String: " + createString + " => Creating")

        zookeeperModifier.createTable(tableNameStr, createString)
        zookeeperModifier.read()

        mapOfCreatedStrings.toStream.foreach(t => System.out.println(t._1 + " " + t._2 + " " + this.runningPort))
    } else if (action.equals(AllowedActions.CreateSql)) {
        System.out.println("Create String: " + createString + " => Creating")

        zookeeperModifier.createTable(tableNameStr, createString)
        zookeeperModifier.read()

        mapOfCreatedStrings.toStream.foreach(t => System.out.println(t._1 + " " + t._2 + " " + this.runningPort))
    } else if (action.equals(AllowedActions.Drop)) {
        System.out.println("Create String: " + createString + " => Dropping")

        zookeeperModifier.dropTable(tableNameStr)
        zookeeperModifier.read()

        mapOfCreatedStrings.toStream.foreach(t => System.out.println(t._1 + " " + t._2 + " " + this.runningPort))
    } else if (action.equals(AllowedActions.DropSql)) {
        System.out.println("Create String: " + createString + " => Dropping")

        zookeeperModifier.dropTable(tableNameStr)
        zookeeperModifier.read()

        mapOfCreatedStrings.toStream.foreach(t => System.out.println(t._1 + " " + t._2 + " " + this.runningPort))
    }

    print("SimpleClusterListener updateZookeeper", runningPort, "end")
  }
  def createNewTableDF(columnNames : Array[String], scenario : Int, joinColumnNames: Array[String]) : DataFrame = {

    var  newDF : DataFrame = null.asInstanceOf[DataFrame]

    scenario match{
      //CSV DataFrame
      case 11 =>
        newDF = csvFileDF.selectExpr(columnNames: _*)
      //Parquet DataFrame
      case 12 =>
        newDF = parquetFileDF.selectExpr(columnNames: _*)
      //Join DataFrame with NO equals Column Names between CSV and Parquet
      case 13=>
        newDF = joiningDataFrames(joinColumnNames).selectExpr(columnNames: _*)
    }
    newDF
  }
  @throws(classOf[NotExistingColumnException])
  def createTableScenario(columnNames : Array[String]): Int = {

    var numberOfColumnsPerDataSource : Array[(String, Int)] = new Array(0)

    val numberOfColumnsTotal : Int = columnNames.length

    val numberOfColumnsInParquet: Int = columnNames.count(t => parquetFileDF.dtypes.toMap.contains(t))
    val numberOfColumnsInCsv: Int = columnNames.count(t => csvFileDF.dtypes.toMap.contains(t))

    if(numberOfColumnsTotal > numberOfColumnsInParquet + numberOfColumnsInCsv){
      val columnsNotFound: Array[String] = columnNames.filter(t => !parquetFileDF.dtypes.toMap.contains(t))
        .filter(s => !csvFileDF.dtypes.toMap.contains(s))

      throw NotExistingColumnException(columnsNotFound = columnsNotFound)

    }else if(numberOfColumnsTotal < numberOfColumnsInParquet + numberOfColumnsInCsv){
      val duplicateColumns: Array[String] = columnNames.filter(t => parquetFileDF.dtypes.toMap.contains(t))
        .filter(s => csvFileDF.dtypes.toMap.contains(s))

      if(numberOfColumnsInParquet==0){
        21 //Impossible Scenario
      }else if(numberOfColumnsInCsv==0){
        22 //Impossible Scenario
      }else{
        23 //Join Scenario with equals Columns in CSV and Parquet
      }
    }else{
      if(numberOfColumnsInParquet==0){
        11 //CSV Scenario
      }else if(numberOfColumnsInCsv==0){
        12 //Parquet Scenario
      }else{
        val columnNamesRepeated: Map[String, String] = anyColumnNameRepeated
        if(columnNamesRepeated.nonEmpty)
          23 //Join Scenario with equals Columns in CSV and Parquet
        else
          13 //Join Scenario with NO equals Columns
      }
    }
  }
  def listTablesSpark() : JsValue = {
    print("SimpleClusterListener listTables Spark", runningPort, "beginning")
    val tableArray = this.sparkSession.catalog.listTables().collect()

    print("SimpleClusterListener listTables", runningPort, "Number of tables in Spark Session "
      .concat(Integer.toString(tableArray.length)))

    this.sparkSession.catalog.listTables().collect().toStream.foreach(t=> System.out.println("Table in Spark Session "
      .concat(t.name.concat(" ").concat(runningPort))))

    print("SimpleClusterListener listTables Spark", runningPort, "end")

    this.sparkSession.catalog.listTables.collect.map(t=>t.name).toJson
  }
  def listTablesZookeeper() : JsValue = {

    print("SimpleClusterListener listTables Zookeeper", runningPort, "end")

    var strArray : Array[String] = new Array(0)

    var i : Int = 0

    while(i<this.zookeeperListener.getZookeeperTables.size()){
      val zookeeperTable : String = this.zookeeperListener.getZookeeperTables.get(i)
      strArray = strArray :+ (zookeeperTable)
      i += 1
    }

    print("SimpleClusterListener listTables", runningPort, "Number of tables in Zookeeper "
      .concat(Integer.toString(strArray.length)))

    strArray.foreach(t=> System.out.println("Table in Zookeeper "
      .concat(t.concat(" ").concat(runningPort))))

    print("SimpleClusterListener listTables Zookeeper", runningPort, "end")

    strArray.toJson

  }
  def getTableName(clientRequest : String, action : AllowedActions) : String = {
    print("SimpleClusterListener getTableName", runningPort, "beginning")

    var tableNameString : String = ""

    if(action.equals(AllowedActions.Create)) {

      val initialIndex = clientRequest.toUpperCase.indexOf(tableLabelCreateEnd)
      val finalIndex : Int = clientRequest.toUpperCase.indexOf(joinLabel)

      if(finalIndex == -1)
        tableNameString = clientRequest.substring(initialIndex + tableLabelCreateEnd.length).trim
      else
        tableNameString = clientRequest.substring(initialIndex + tableLabelCreateEnd.length, finalIndex).trim

    }else if(action.equals(AllowedActions.CreateSql)) {

      tableNameString = getTableNameSql(clientRequest, AllowedActions.CreateSql)

    }else if(action.equals(AllowedActions.Drop)){

      val finalIndex = clientRequest.toUpperCase.indexOf(tableLabelDrop)
      tableNameString = clientRequest.substring(finalIndex + tableLabelDrop.length).trim

    }else if(action.equals(AllowedActions.DropSql)) {
      tableNameString = getTableNameSql(clientRequest, AllowedActions.DropSql)

    }

    print("SimpleClusterListener getTableName", runningPort, "end")
    tableNameString
  }

  def getTableNameSql(clientRequest : String, action : AllowedActions): String ={

    var tableNameString : String = null.asInstanceOf[String]

    if(action.equals(AllowedActions.CreateSql)) {
      val split = clientRequest.split("\\s+")
      var found: Boolean = false
      tableNameString = split.reduce((x, y) => {
        var returnStr: String = y
        if (!found) {
          if (x.equalsIgnoreCase("TABLE") && !y.equalsIgnoreCase("IF")) {
            returnStr = y
            found = true
          } else if (x.equalsIgnoreCase("EXISTS")) {
            returnStr = y
            found = true
          }
        } else {
          returnStr = x
        }
        returnStr
      })
    }else if(action.equals(AllowedActions.DropSql)){
      val split : Array[String] = clientRequest.split("\\s+")
      tableNameString = split(split.length-1)
    }
    tableNameString
  }

  def getColumnNames(clientRequest : String) : Array[String] = {
    print("SimpleClusterListener getColumnNames", runningPort, "beginning")

    val initialIndex = clientRequest.toUpperCase.indexOf(columnsLabel)
    val finalIndex = clientRequest.toUpperCase.indexOf(tableLabelCreateEnd)

    val columnNamesString = clientRequest.substring(initialIndex + columnsLabel.length , finalIndex).trim
    val columnNames = columnNamesString.split(",").map(t=> t.trim)
    print("SimpleClusterListener getColumnNames", runningPort, "end")
    columnNames
  }
  def getJoinColumnNames(clientRequest : String) : Option[Array[String]] = {

    val initialIndex = clientRequest.toUpperCase.indexOf(joinLabel)
    val finalIndex = clientRequest.length

    val joinColumnNamesString = clientRequest.substring(initialIndex + joinLabel.length , finalIndex).trim
    val joinColumnNames = joinColumnNamesString.split("-").map(t=> t.trim)

    if(parquetFileDF.dtypes.toMap.contains(joinColumnNames(0))&&csvFileDF.dtypes.toMap.contains(joinColumnNames(1)))
      Some(joinColumnNames)
    else
      None
  }

  import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

  @throws(classOf[NoSuchTableException])
  @throws(classOf[Exception])
  def querySentenceArray(clientRequest : String): JsValue = {
    try {
      //var jsReturned : JsValue = "Done".toJson

      print("SimpleClusterListener querySentenceArray", runningPort, "beginning")
      print("SimpleClusterListener querySentenceArray clientRequest", runningPort, clientRequest)

      val jsvalue : JsArray = JsArray(this.sparkSession.sql(clientRequest).collect().toVector.map(t=> convertRowToJSON(t)))

      print("SimpleClusterListener querySentenceArray", runningPort, "end")

      jsvalue
    }catch{
      case e: NoSuchTableException =>
        throw e
      case e: Exception =>
        throw e
    }
  }
  def convertRowToJSON(row: Row): JsValue = {

    val newRow: scala.collection.mutable.Map[String, JsValue] = scala.collection.mutable.HashMap[String, JsValue]()

    val m = row.getValuesMap(row.schema.fieldNames)

    for (a <- m.keySet) {
      val element: Any = m(a)
      var arr: Array[Byte] = null.asInstanceOf[Array[Byte]]

      element match {
        case element : Array[Byte] =>
          val arr: Array[Byte] = m(a)
          newRow.put(a, new String(arr).toJson)
        case element : Int =>
          newRow.put(a, element.asInstanceOf[Int].toJson)
        case element : Double =>
          newRow.put(a, element.asInstanceOf[Double].toJson)
        case element : Long =>
          newRow.put(a, element.asInstanceOf[Long].toJson)
        case element : String =>
          newRow.put(a, element.asInstanceOf[String].toJson)
        case element: Boolean =>
          newRow.put(a, element.asInstanceOf[Boolean].toJson)
        case _ =>
          newRow.put(a, null.asInstanceOf[JsValue])
      }
    }
    newRow.toMap.toJson
  }
  def getColumnsParquetJson: JsValue = {
    parquetFileDF.dtypes.toJson
  }
  def getColumnsCsvJson: JsValue = {
    csvFileDF.dtypes.toJson
  }
  def getColumns: Array[(String,String)]= {
    parquetFileDF.dtypes
  }
  def print(method: String, port: String, trace: String): Unit = {
    System.out.println("#############" + method + " " + port + " " + trace + "#############")
  }
}