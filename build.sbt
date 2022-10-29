ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "affix-zone-int"
  )
name := "spark-sftp"

organization := "com.springml"

ThisBuild / scalaVersion := "2.13.8"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "com.springml" % "sftp.client" % "1.0.3",
  "org.mockito" % "mockito-core" % "4.8.0",
  "com.databricks" % "spark-xml" % "0.5.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.2.14" % "test",
  "org.apache.spark" %% "spark-avro" % "3.3.1",
  "org.apache.spark" %% "spark-hive" % "3.2.2"%"test"
)
