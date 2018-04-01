package cs4240

import org.apache.spark.sql.SparkSession

object CommentAnalysis {

  def run(): Unit = {
    val sparkSession = SparkSession.builder.appName("cs4240-comment-analysis").getOrCreate

    // Pointing to a parent folder gets all the parquet files in that folder
    val commentInfo = sparkSession.read.parquet(BigQueryImporter.commentInfoRoot)

    commentInfo.take(10).foreach(println)

    sparkSession.stop()
  }
}
