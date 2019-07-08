package io.blue.config

import java.io.File
import com.typesafe.scalalogging._
import scala.collection.JavaConverters._
import java.io.{InputStream, File, FileInputStream}
import org.yaml.snakeyaml._
import org.yaml.snakeyaml.constructor._

import io.blue.config.Constants.ConnectorType
import io.blue.connector._

object Config extends LazyLogging{

  private def parseApp(app: Object) : App = {
    logger.trace("Parsing config [app]")
    val options = app.asInstanceOf[java.util.Map[String, Object]]
    var a = new App
    a.parallel = options.get("parallel").asInstanceOf[Int]
    a
  }

  private def parseConnectors(connectors: Object): List[Connector] = {
    logger.trace("Parsing config [connectors]")
    connectors.asInstanceOf[java.util.List[java.util.Map[String, Object]]].asScala.toList.map{ connector =>
      val connectorName = connector.get("name").toString
      val parallel = connector.get("parallel").asInstanceOf[Int]
      connector.get("type").toString.toUpperCase match {
        case ConnectorType.ORACLE_ROWID =>
          var c = new OracleRowidConnector
          c.name = connectorName
          c.parallel = parallel
          c.connectorType = ConnectorType.ORACLE_ROWID
          c.url = connector.get("url").toString
          c.username = connector.get("username").toString
          c.password = connector.get("password").toString
          c.driverClass = connector.get("driver_class").toString
          c
        case ConnectorType.JDBC =>
          var c = new JdbcConnector
          c.parallel = parallel
          c.name = connectorName
          c.url = connector.get("url").toString
          c.username = connector.get("username").toString
          c.password = connector.get("password").toString
          c.driverClass = connector.get("driver_class").toString
          c
        case ConnectorType.FILE =>
          var c = new FileConnector
          c.parallel = parallel
          c.name = connectorName
          c.path = connector.get("path").toString
          c.fieldDelimiter = connector.get("field_delimiter").toString
          c.recordSeperator = connector.get("record_seperator").toString
          c
        case _ => throw new RuntimeException("Unsupported connector.")
      }
    }
  }

  def searchConfigFile: Option[File] = {
    var file = new File("lauda.yaml")
    if (file.exists) { Some(file) } else {
      file = new File("config/lauda.yaml")
      if (file.exists) { Some(file) } else { None }
    }
  }

  def parse(configFile: File): Config = {
    logger.trace(s"Parsing config file ${configFile}")
    var config = new Config
    val result = (new Yaml)
      .load(new FileInputStream(configFile))
      .asInstanceOf[java.util.Map[String, Object]]

    for(name <- result.keySet.toArray) {
      name match {
        case "app" =>
          config.app = parseApp(result.get(name))
        case "connectors" =>
          var connectors = result.get(name).asInstanceOf[java.util.Map[String, Object]]
          config.sourceConnectors = parseConnectors(connectors.get("source"))
          config.targetConnectors = parseConnectors(connectors.get("target"))
      }
    }
    config
  }
}

class Config {
  var app: App = _
  var sourceConnectors: List[Connector] = _
  var targetConnectors: List[Connector] = _

  def getSourceConnector(name: String): Connector = {
    val connector = sourceConnectors find (_.name == name)
    if (connector.isEmpty) {
      throw new RuntimeException(s"Connector does not exists ${name}")
    }
    connector.get
  }

  def getTargetConnector(name: String): Connector = {
    val connector = targetConnectors find (_.name == name)
    if (connector.isEmpty) {
      throw new RuntimeException(s"Connector does not exists ${name}")
    }
    connector.get
  }
}
