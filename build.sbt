name := "flume-extensions"

organization := "com.avvo.data"

version := "0.1"

scalaVersion := "2.11.0"

resolvers += "ClouderaRepository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.apache.flume" % "flume-ng-core" % "1.6.0",
  "org.apache.flume" % "flume-ng-configuration" % "1.6.0",
  "org.apache.flume" % "flume-ng-sdk" % "1.6.0",
  "org.apache.avro" % "avro-tools" % "1.7.6-cdh5.5.5",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.5.5",
  "net.fosdal" %% "oslo" % "0.2"
)

test in assembly := {}
assemblyMergeStrategy in assembly := {
  case "reference.conf"                    => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _                                   => MergeStrategy.first
}

//
// Compiler Settings
//
// format: off
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-value-discard"
)