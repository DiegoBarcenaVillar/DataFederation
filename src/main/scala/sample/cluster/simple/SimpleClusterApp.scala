package sample.cluster.simple

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.client.ClusterClientReceptionist



object SimpleClusterApp {
  
  def main(args: Array[String]): Unit = {



    if (args.isEmpty)
      startup(Seq("2551", "2552", "4579"))
    else
      startup(args)
  }

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      // Override the configuration of the port

      /*
      val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        akka.remote.artery.canonical.port=$port
        """).withFallback(ConfigFactory.load())
        */

      val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        """).withFallback(ConfigFactory.load())


      // Create an Akka system
      val system = ActorSystem("ClusterSystem", config)
      // Create an actor that handles cluster domain events
      val actor = system.actorOf(Props[SimpleClusterListener], name = "clusterListener" + port)

      if(port.equals("4579"))
        ClusterClientReceptionist(system).registerService(actor)

    }
  }

  
}