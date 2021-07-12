name := "spark-utils"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.1"
val deltaVersion = "0.7.0"
val majorVersion = "1"
val minorVersion = "0"
version := s"${majorVersion}.${minorVersion}"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "io.delta" %% "delta-core" % deltaVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.14.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.46"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "_" + sparkVersion + "_" + deltaVersion + "_" + module.revision + "." + artifact.extension
}

