name := "deltalake-cdc-utils"

version := "0.1"

scalaVersion := "2.13.10"


libraryDependencies ++= Seq(

  "io.delta" %% "delta-core" % "2.1.1",
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-avro" % "3.3.0"
)