package com.zefe.partition
import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hola").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    val schemaSource = scala.io.Source.fromFile("src/main/resources/schema_individual.json").getLines.mkString
    val schema = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    val writer = new BufferedWriter(
      new FileWriter(
        "src/main/resources/individual_contributions_2.csv"
      )
    )
    val df = spark.read.format("csv")
      .option("delimiter",",")
      .option("quote","")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", "true")
      .schema(schema)
      .load("src/main/resources/individual_contributions.csv")

    df.show(10,true)


  }

  def read(mpath: String): List[String] ={
    scala.io.Source.fromFile("this.res.pathNamings")
      .getLines()
      .toList
  }

}
