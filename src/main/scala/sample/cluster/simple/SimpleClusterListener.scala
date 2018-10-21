package sample.cluster.simple

import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.ActorLogging
import akka.actor.Actor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame


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
      sender ! processString(msg)
    case _ =>
  }

  def processString (sqlText:String ) :  String /*DataFrame*/ = {
    //sparkSession.sql(sqlText)
    "OK"
  }
}