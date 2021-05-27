package com.gsk.kg.bellman_tools

import org.scalatest.FlatSpec
import com.gsk.kg.engine.syntax._

class UtilsSpec extends FlatSpec with SparkSessionWrapper {

  "nt to df" should "return correct results" in {

    val q1 =
      """
        | SELECT ?s ?p ?o
        | { ?s ?p ?o }
        | LIMIT 6
        |""".stripMargin

    val path = getClass.getResource("/testdata/input.nt").getPath
    val df = Utils.ntToDf(path)
    implicit val sqlctx = spark.sqlContext
    assert(df.sparql(q1).count() == 6)
  }
}
