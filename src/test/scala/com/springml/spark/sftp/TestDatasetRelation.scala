package com.springml.spark.sftp

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterEach}

/**
 * Simple unit test for basic testing on different formats of file
 */
class TestDatasetRelation extends AnyFlatSpec with BeforeAndAfterEach {
  var ss: SparkSession = _

  override def beforeEach() {
    ss = SparkSession.builder().master("local").enableHiveSupport().appName("Test Dataset Relation").getOrCreate()
  }

   "Read CSV"  should "success" in {
    val fileLocation = getClass.getResource("/sample.csv").getPath
    val dsr = DatasetRelation(fileLocation, "csv", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  "Read CSV using custom delimiter"  should "success" in {
    val fileLocation = getClass.getResource("/sample.csv").getPath
    val dsr = DatasetRelation(fileLocation, "csv", "false", "true", ";", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  "Read multiline CSV using custom quote and escape"   should "success" in {
    val fileLocation = getClass.getResource("/sample_quoted_multiline.csv").getPath
    val dsr = DatasetRelation(fileLocation, "csv", "false", "true", ",", "\"", "\\", "true", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }


  "Read JSON"   should "success" in {
    val fileLocation = getClass.getResource("/people.json").getPath
    val dsr = DatasetRelation(fileLocation, "json", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  "Read AVRO"   should "success" in {
    val fileLocation = getClass.getResource("/users.avro").getPath
    val dsr = DatasetRelation(fileLocation, "avro", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
  }

 "Read parquet"   should "success" in {
    val fileLocation = getClass.getResource("/users.parquet").getPath
    val dsr = DatasetRelation(fileLocation, "parquet", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
  }

  "Read text file"   should "success" in{
    val fileLocation = getClass.getResource("/plaintext.txt").getPath
    val dsr = DatasetRelation(fileLocation, "txt", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  "Read xml file"   should "success" in {
    val fileLocation = getClass.getResource("/books.xml").getPath
    val dsr = DatasetRelation(fileLocation, "xml", "false", "true", ",", "\"", "\\", "false", "book", null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(12 == rdd.count())
  }
   "Read orc file"   should "success" in {
    val fileLocation = getClass.getResource("/books.orc").getPath
    val dsr = DatasetRelation(fileLocation, "orc", "false", "true", ",", "\"", "\\", "false", "book", null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(12 == rdd.count())
  }
}
