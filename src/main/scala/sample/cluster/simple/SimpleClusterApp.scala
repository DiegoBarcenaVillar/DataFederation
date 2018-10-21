package sample.cluster.simple

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.client.ClusterClientReceptionist

object SimpleClusterApp {
  
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils_hadoop2.6.0\\");

    if (args.isEmpty)
      startup(Seq("2551", "2552", "5000"))
    else
      startup(args)
  }

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>

      // Override the configuration of the port
      val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        """).withFallback(ConfigFactory.load())

      // Create an Akka system
      val system = ActorSystem("ClusterSystem", config)

      // Create an actor that handles cluster domain events
      val actor = system.actorOf(Props[SimpleClusterListener], name = "clusterListener")

      if(port.equals("5000"))
        ClusterClientReceptionist(system).registerService(actor)
    }
  }
}