name := "spark-utils1"

version := "0.1"

scalaVersion := "2.12.15"
val sparkVersion = "3.0.1"
val deltaVersion = "0.7.0"
val majorVersion = "1"
val minorVersion = "1"
version := s"${majorVersion}.${minorVersion}"

scalacOptions ++= Seq("-Xcheckinit")

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "com.crealytics" % "spark-excel_2.12" % "3.1.2_0.17.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.8.2"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.419"
libraryDependencies += "io.delta" %% "delta-core" % deltaVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "com.typesafe" % "config" % "1.4.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "_" + sparkVersion + "_" + deltaVersion + "_" + module.revision + "." + artifact.extension
}
