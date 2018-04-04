package cs4240

import org.apache.spark.sql.SparkSession

object CommentAnalysis {

  def run(fullyQualifiedInputTableIds: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.appName("cs4240-comment-analysis").getOrCreate

    fullyQualifiedInputTableIds.foreach(fullyQualifiedInputTableId => {
      println(f"Processing $fullyQualifiedInputTableId ...")

      // Pointing to a parent folder gets all the parquet files in that folder
      val commentInfo = sparkSession.read.parquet(BigQueryImporter.commentInfoLocation(fullyQualifiedInputTableId))

      println("Sample:")
      commentInfo.take(20).foreach(println)

      println(f"Count: ${commentInfo.count()}")
    })

    sparkSession.stop()
  }
}
