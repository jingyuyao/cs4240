name := "cs4240project"

version := "0.1"

scalaVersion := "2.11.8"

assemblyJarName := "cs4240.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
libraryDependencies += "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.12.0-hadoop2" % "provided"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.2" % "provided"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0"
