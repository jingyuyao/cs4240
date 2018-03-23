name := "cs4240project"

version := "0.1"

scalaVersion := "2.11.8"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "cs4240.jar" }

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.12.0-hadoop2"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.2"
