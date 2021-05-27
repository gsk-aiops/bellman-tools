package com.gsk.kg.bellman_tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  implicit lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .config(new SparkConf())
      .getOrCreate()
  }
}
