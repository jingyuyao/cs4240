package cs4240

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetImporter {
  private val sparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("cs4240-importer")
      .getOrCreate()

  def run(fullyQualifiedInputTableId: String): DataFrame =
    sparkSession.read.parquet(BigQueryImporter.commentInfoLocation(fullyQualifiedInputTableId))
}
