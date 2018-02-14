package com.aispeech

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tianyi on 14/02/2018.
  */
object Hw {

  case class Person(name:String,age:Int)
  var schemaArr:List[(String, Int)] = List(
    ("eventName", 0),
    ("sessionId", 0)
  )

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("check first app")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    //val rdd = sc.textFile("/data2/user_data/his/data/djm/scala/data/json.txt")
    //val rdd = sc.textFile("/Users/tianyi/speech/testScala/data/json.txt")
    val df = hc.sql("select json from his.ba_online_prod  limit 10")
    //val df = sqlContext.sql("select json from his.ba_online_prod  limit 10")
    val resDf = df.map(ele => {
      try {
        parseJsonMain(ele(1).toString)
        Row(ele(0).toString.size)
      } catch {
        case e => {
          println(e)
          Row(ele(0).toString.size)
        }
      }
    })
    val structType = StructType(
      Array(
        StructField("length", IntegerType, true)
      )
    )
    //val structType = StructType(schemaArr.map(fieldName => StructField(fieldName._1, StringType)))
    //var df = sqlContext.createDataFrame(resDf, structType)
    println("length : ", resDf.filter(f => f.length > 0).count())
    val resAllData = sqlContext.createDataFrame(resDf.filter(f => f.length > 0), structType)
    println("count is ", df.count())
    resAllData.show
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
      println(schemaName)
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
