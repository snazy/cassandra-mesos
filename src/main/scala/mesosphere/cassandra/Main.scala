package mesosphere.cassandra

import org.yaml.snakeyaml.Yaml
import java.io.FileReader
import java.util
import scala.collection.JavaConverters._
import org.apache.commons.cli.MissingArgumentException
import java.net.InetAddress
import org.apache.log4j.{Level, BasicConfigurator}
import mesosphere.utils.{StateStore, Slug}
import org.apache.mesos.state.ZooKeeperState
import java.util.concurrent.TimeUnit
import org.rogach.scallop._

/**
 * Mesos on Cassandra
 * Takes care of most of the "annoying things" like distributing binaries and configuration out to the nodes.
 *
 * @author erich<IDonLikeSpam>nachbar.biz
 */
object Main extends App with Logger {

  val yaml = new Yaml()
  val mesosConf = yaml.load(new FileReader("conf/mesos.yaml"))
    .asInstanceOf[util.LinkedHashMap[String, Any]].asScala

  val cassConf = yaml.load(new FileReader("conf/cassandra.yaml"))
    .asInstanceOf[util.LinkedHashMap[String, Any]].asScala

  // Get configs out of the mesos.yaml file
  val execUri = mesosConf.getOrElse("mesos.executor.uri",
    throw new MissingArgumentException("Please specify the mesos.executor.uri")).toString

  val masterUrl = mesosConf.getOrElse("mesos.master.url",
    throw new MissingArgumentException("Please specify the mesos.master.url")).toString

  val javaLibPath = mesosConf.getOrElse("java.library.path",
    "/usr/local/lib/libmesos.so").toString
  System.setProperty("java.library.path", javaLibPath)

  val numberOfHwNodes = mesosConf.getOrElse("cassandra.noOfHwNodes", 1).toString.toInt

  val zkStateServers = mesosConf.getOrElse("state.zk", "localhost:2181/cassandra-mesos").toString

  val confServerPort = mesosConf.getOrElse("cassandra.confServer.port", 8282).toString.toInt

  val confServerHostName = mesosConf.getOrElse("cassandra.confServer.hostname",
    InetAddress.getLocalHost().getHostName()).toString

  // Find all resource.* settings in mesos.yaml and prep them for submission to Mesos
  val resources = mesosConf.filter {
    _._1.startsWith("resource.")
  }.map {
    case (k, v) => k.replaceAllLiterally("resource.", "") -> v.toString.toFloat
  }

  //TODO erich load the Cassandra log4j-server.properties file
  BasicConfigurator.configure()
  getRootLogger.setLevel(Level.INFO)

  val conf = new Conf(args)

  // Get the cluster name out of the cassandra.yaml
  val clusterName = cassConf.get("cluster_name").get.toString
  val state = new ZooKeeperState(
    zkStateServers,
    20000,
    TimeUnit.MILLISECONDS,
    "/cassandraMesos/" + Slug(clusterName)
  )

  val store = new StateStore(state)
  val killRegex = conf.kill.get

  // Instanciate framework and scheduler
  val scheduler = new CassandraScheduler(masterUrl,
    execUri,
    confServerHostName,
    confServerPort,
    resources,
    numberOfHwNodes,
    clusterName)(store)

  scheduler.suspendScheduling = killRegex.isDefined

  val schedThred = new Thread(scheduler)
  schedThred.start()
  scheduler.waitUnitInit

  // find out if we kill tasks or start up normally
  killRegex match {
    case Some(regex) =>
      // Kill existing Cassandra tasks
      info("Killing Cassandra tasks")
      val tasks = scheduler.kill(regex)
      schedThred.interrupt()
      scheduler.stop()
      info(s"Killed tasks: ${tasks}")

    case _ =>
      // Just start Cassandra.

      info("Starting Cassandra on Mesos.")

      info("Cassandra nodes starting on: " + scheduler.fetchNodeSet().mkString(","))

  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner( """Usage: cassandra-mesos [OPTION]...
            |Run Cassandra on Mesos.
            |Options:
            | """.stripMargin)
  val kill = opt[String](descr = """Kills tasks by regex matching hostnames or task IDs. Kill all tasks by using '.*'. Make sure to enclose it in single quotes.""")
}
