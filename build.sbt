name := "testScala"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "com.alibaba" % "fastjson" % "1.2.12",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
