package cs4240

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetImporter {
  private val sparkSession =
    SparkSession
      .builder()
      .appName("cs4240-importer")
      .config("spark.master", "local")
      .getOrCreate()

  def run(fullyQualifiedInputTableId: String): DataFrame =
    sparkSession.read.parquet(BigQueryImporter.commentInfoLocation(fullyQualifiedInputTableId))
}
