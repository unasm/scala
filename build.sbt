name := "testScala"

version := "1.0"

scalaVersion := "2.10.5"
//scalaVersion := "2.11.0"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.apache.spark" %% "spark-hive" % "1.6.3",
  "org.apache.spark" %% "spark-hive-thriftserver" % "1.6.3",
  "com.alibaba" % "fastjson" % "1.2.12",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
