import Dependencies._


ThisBuild / scalaVersion     := "2.13.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "io.blue"
ThisBuild / organizationName := "blue"

lazy val root = (project in file("."))
  .settings(
    trapExit := false,
    name := "lauda",
    libraryDependencies += scalaTest % Test
  )

unmanagedJars in Compile ++= (file("lib/") * "*.jar").classpath

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "1.24",
  "info.picocli" % "picocli" % "4.0.0-beta-2",
  "org.slf4j" % "slf4j-api" % "1.7.26",
  "org.slf4j" % "slf4j-simple" % "1.7.26",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "me.tongfei" % "progressbar" % "0.6.0",
  "com.typesafe.akka" %% "akka-actor" % "2.5.23"
)
scalacOptions ++= Seq("-optimize")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}