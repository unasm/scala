package com.aispeech

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

/**
  * Created by tianyi on 14/02/2018.
  */
object hw {

  var schemaArr = List(
    ("eventName", 0),
    ("sessionId", 0),
    ("recordId", 0),
    ("contextId", 0),
    ("logTime", 0),
    ("message_authorizer_access_token", 1),
    ("message_method", 0),
    ("message_output_status", 0)
  )

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("check first app")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/Users/tianyi/speech/testScala/data/json.txt")
    println("count is ", rdd.count())
    println("Hi!")
    System.out.println("testing")
    Row()
  }

  def parseJsonMain(json: String): Row = {
    val values = new Array[String](schemaArr.size)
    this.parseJsonDepth(json, "", 0, values)
    Row.fromSeq(values)
  }
  def parseJsonDepth(json: String, prefix: String, depth: Int, values : Array[String]) {
    val iterator = JSON.parseObject(json).entrySet().iterator()
    while (iterator.hasNext) {
      val node = iterator.next()
      val dataValue =  node.getValue
      var schemaName  =  node.getKey
      if (prefix != "") {
        schemaName = prefix + "__" + schemaName
      }
      val dataType = dataValue.getClass.getSimpleName
      if (dataType == "String" || dataType == "Integer") {
        val schemaIdx = schemaArr.map(r => r._1).indexOf(schemaName)
        if (schemaIdx != -1) {
          println("updateing : " +  schemaName)
          values.update(schemaIdx, dataValue.toString)
        } else {
          println("schema_not_exists : ", schemaName, " : ", dataType)
        }
      } else if (dataType == "JSONObject") {
        if (depth < 2) {
          this.parseJsonDepth(dataValue.toString, schemaName, depth + 1, values)
        }
      } else {
        println("missing_key, ", schemaName, dataType)
      }
    }
  }
}
