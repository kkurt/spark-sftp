package com.springml.spark.sftp

import org.apache.log4j.Logger

/**
 * Delete the temp file created during spark shutdown
 */
class DeleteTempFileShutdownHook(
    fileLocation: String) extends Thread {

  private val logger = Logger.getLogger(classOf[DatasetRelation])

  override def run(): Unit = {
    logger.info("Deleting " + fileLocation )
    println("Deleting " + fileLocation )
    //FileUtils.deleteQuietly(new File(fileLocation))
  }
}