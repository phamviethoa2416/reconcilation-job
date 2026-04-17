ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

val sparkVersion = "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "ReconciliationJob",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-avro" % sparkVersion,
      "com.oracle.database.jdbc" % "ojdbc8" % "19.3.0.0",
    )
  )
