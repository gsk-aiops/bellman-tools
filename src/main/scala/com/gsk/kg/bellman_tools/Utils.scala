package com.gsk.kg.bellman_tools

import org.apache.spark.sql.{DataFrame}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang

object Utils extends SparkSessionWrapper {
  org.apache.jena.query.ARQ.init()
  implicit val s = spark.sqlContext
  /**
   * Converts a valid NT file to a dataframe
   * @param i Path to input file
   * @return Dataframe with columns s,p,o
   */
  def ntToDf(i:String):DataFrame = {
    spark.read.rdf(Lang.NTRIPLES)(i)
  }

  /**
   * More tools to come!
   */
}