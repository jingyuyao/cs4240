package cs4240

import org.apache.spark.sql.SparkSession

object ParquetImporter {

  def run(fullyQualifiedInputTableId: String): Unit = {
    val sparkSession = SparkSession.builder.appName("cs4240-parquet-importer").getOrCreate

    val commentInfo = sparkSession.read.parquet(BigQueryImporter.commentInfoLocation(fullyQualifiedInputTableId))

    commentInfo.take(10).foreach(println)

    sparkSession.stop()
  }
}
