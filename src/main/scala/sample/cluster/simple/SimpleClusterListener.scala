package sample.cluster.simple

import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.ActorLogging
import akka.actor.Actor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
//import scala.util.parsing.json.JSONObject
//import org.apache.spark.sql._





class SimpleClusterListener extends Actor with ActorLogging {


  val cluster = Cluster(context.system)

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("datafederation session")
    .getOrCreate()




  // subscribe to cluster changes, re-subscribe when restart 
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {

    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case msg:String =>
      log.info(msg)
      sender ! processString(msg).toJSON.collect().mkString(";")
    case _ =>
  }

  def processString (sqlText:String ) :  /*String*/ DataFrame = {

    try {
      //parkSession.sql(sqlText)
      dummyDataFrame
    }catch {
      case ex: org.apache.spark.sql.catalyst.parser.ParseException => {
        ex.printStackTrace()
        null
      }
    }
  }

  def dummyDataFrame () :  DataFrame = {

    import sparkSession.implicits._

    val df = Seq((1,2,3),(11,12,13)).toDF("A", "B", "C")
    df

  }



}