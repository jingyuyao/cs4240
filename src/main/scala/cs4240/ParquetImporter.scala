package cs4240

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetImporter {

  def run(fullyQualifiedInputTableId: String): DataFrame = {
    val sparkSession = SparkSession.builder.appName("cs4240-importer").getOrCreate

    sparkSession.read.parquet(BigQueryImporter.commentInfoLocation(fullyQualifiedInputTableId))
  }
}
