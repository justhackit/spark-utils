package com.sparkutils.io

object SampleS3Files {
  def sampleS3Files(s3RootPath:String)={
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}

    import java.net.URI
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
