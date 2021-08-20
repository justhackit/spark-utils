package com.sparkutils.io

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SampleS3Files {
  def sampleS3Files(s3RootPath:String)={
    import java.net.URI
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.conf.Configuration
    import java.time.LocalDate

    val filesSelected = scala.collection.mutable.ListBuffer.empty[String]
    val start = LocalDate.of(2021, 7, 1)
    for (i <- 0 to 31) {
      val filePath = s"$s3RootPath/process_date=${start.plusDays(i).toString}/"
      val fileSystem = FileSystem.get(URI.create(filePath), new Configuration())
      val it = fileSystem.listFiles(new Path(filePath), false)
      var sampleFilesCount = 0
      while (it.hasNext() && sampleFilesCount <=4) {
        val flName = it.next().getPath
        if (flName.getName.endsWith((".parquet"))) {
          filesSelected += flName.toString
          sampleFilesCount= sampleFilesCount +1
        }

      }

    }
    filesSelected.foreach(println)
  }
}
