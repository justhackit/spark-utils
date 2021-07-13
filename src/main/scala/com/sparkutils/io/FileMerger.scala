package com.sparkutils.io

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ListObjectsRequest}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable._
import scala.collection.parallel._
import scala.concurrent.forkjoin.ForkJoinPool

/*
Usage : spark-submit ..... --class com.sparkutils.io.FileMerger s3://my-src-bucket/prod/order_history/ s3://my-src-bucket/prod/order_history_merged/ 134217728
 */
object FileMerger {
  val className = getClass.getCanonicalName.replace("$", "")
  val logger = Logger.getLogger(className)
  logger.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    //Job args
    val (inputBucket, prefix) = getBucketNameAndPrefix(args(1))
    val targetDirectory = args(2)
    val maxIndividualMergedFileSize = args(3).toLong

    val sparkConf = new SparkConf().setAppName(className)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val inputDirs = listDirectoriesInS3(inputBucket, prefix).map(prefix => "s3://" + inputBucket + "/" + prefix)
    logger.info(s"Total directories found : ${inputDirs.size}")
    val startedAt = System.currentTimeMillis()
    //You may want to tweak the following to set how many input directories to process in parallel
    val forkJoinPool = new ForkJoinPool(inputDirs.size)
    val parallelBatches = inputDirs.par
    parallelBatches.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
    parallelBatches.foreach(dir => {
      val (srcBkt, prefix) = getBucketNameAndPrefix(dir)
      logger.info(s"Working on ::=> SourceBkt : $srcBkt,Prefix:$prefix")
      //Step 1 : Get file sizes
      val fileSizesMap = getFileSizes(inputBucket, prefix)
      //Step 2 : Group them based on target file size
      val grouped = makeMergeBatches(fileSizesMap, maxIndividualMergedFileSize)
      //Step 3 : Print stats
      printMergedSizes(grouped)
      //Step 4 : Strip sizes
      val sizedStripped = stripSizesFromFileNames(grouped)
      //Step 5 : Make final file names to read from
      val finalSourceFileNames = addS3BucketNameToFileNames(sizedStripped, bucketName = inputBucket)
      //Step 6 : Merge files and write them out
      mergeFiles(spark, finalSourceFileNames, targetDirectory)
    })
    logger.info(s"Merging completed and total time taken : ${(System.currentTimeMillis() - startedAt) / (1000 * 60)}")
  }

  def getFileSizes(bucketName: String, prefix: String): scala.collection.immutable.Map[String, Long] = {
    val s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
    val listing = s3.listObjectsV2(bucketName, prefix)
    val files = listing.getObjectSummaries.asScala.map(_.getKey).filter(!_.split("/").last.startsWith("_"))
    val filesSizeMap = collection.mutable.Map[String, Long]()
    files.foreach(obj => {
      val meta = s3.getObjectMetadata(new GetObjectMetadataRequest(bucketName, obj))
      filesSizeMap += (obj -> meta.getContentLength)
    })
    filesSizeMap.toMap
  }

  def listDirectoriesInS3(bucketName: String, prefix: String): List[String] = {
    val s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix).withDelimiter("/")
    s3.listObjects(listObjectsRequest).getCommonPrefixes.asScala.toList
  }

  def sizeOfThisBatch(batch: ListBuffer[String]): Long = {
    batch.map(_.split('|').last.toInt).sum
  }

  def printMergedSizes(grouped: ListBuffer[ListBuffer[String]]): Unit = {
    grouped.foreach(batch => {
      logger.info(s"FileCount # ${batch.size}, Size : ${FileUtils.byteCountToDisplaySize(sizeOfThisBatch(batch))}")
    })
  }

  def stripSizesFromFileNames(grouped: ListBuffer[ListBuffer[String]]): ListBuffer[ListBuffer[String]] = {
    val groupedFiles = ListBuffer[ListBuffer[String]]()
    grouped.foreach(batch => {
      val newBatch = ListBuffer[String]()
      batch.foreach(newBatch += _.split('|').head)
      groupedFiles += newBatch
    })
    groupedFiles
  }

  def addS3BucketNameToFileNames(grouped: ListBuffer[ListBuffer[String]], bucketName: String): ListBuffer[ListBuffer[String]] = {
    val groupedFiles = ListBuffer[ListBuffer[String]]()
    grouped.foreach(batch => {
      val newBatch = ListBuffer[String]()
      batch.foreach(file => {
        newBatch += "s3://" + bucketName.stripSuffix("/") + "/" + file.split('|').head.stripSuffix("/")
      })
      groupedFiles += newBatch
    })
    groupedFiles
  }

  def getBucketNameAndPrefix(fullPath: String): (String, String) = {
    val bucketName = fullPath.substring(5).split("/").head
    val prefix = fullPath.stripPrefix("s3://" + bucketName + "/")
    (bucketName, prefix)
  }

  def makeMergeBatches(fileSizesMap: scala.collection.immutable.Map[String, Long], maxTargetFileSize: Long): ListBuffer[ListBuffer[String]] = {
    //val sortedFileSizes = ListMap(fileSizesMap.toSeq.sortBy(_._2.toInt):_*).toMap
    val sortedFileSizes = fileSizesMap.toSeq.sortBy(_._2)
    val groupedFiles = ListBuffer[ListBuffer[String]]()
    groupedFiles += ListBuffer[String]()
    for (aFile <- sortedFileSizes) {
      val lastBatch = groupedFiles.last
      if ((sizeOfThisBatch(lastBatch) + aFile._2) < maxTargetFileSize) {
        lastBatch += aFile._1 + "|" + aFile._2.toString
      } else {
        val newBatch = ListBuffer[String]()
        newBatch += aFile._1 + "|" + aFile._2.toString
        groupedFiles += newBatch
      }
    }
    groupedFiles
  }

  def mergeFiles(spark: SparkSession, grouped: ListBuffer[ListBuffer[String]], targetDirectory: String): Unit = {
    val startedAt = System.currentTimeMillis()
    val forkJoinPool = new ForkJoinPool(grouped.size)
    val parllelBatches = grouped.par
    parllelBatches.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
    parllelBatches foreach (batch => {
      logger.debug(s"Merging ${batch.size} files into one")
      try {
        spark.read.parquet(batch.toList: _*).coalesce(1).write.mode("append").parquet(targetDirectory.stripSuffix("/") + "/")
      } catch {
        case e: Exception => logger.error(s"Error while processing batch $batch : ${e.getMessage}")
      }
    })
    logger.debug(s"Total Time taken to merge this directory: ${(System.currentTimeMillis() - startedAt) / (1000 * 60)} mins")
  }

//The End
}
