import sbt.project

name := "Scala-Spark"

version := "0.1"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
)
