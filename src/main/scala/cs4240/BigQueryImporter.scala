package cs4240

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BigQueryImporter {
  private val sparkSession =
    SparkSession
      .builder()
      .appName("cs4240-importer")
      .config("spark.master", "local")
      .getOrCreate()
  import sparkSession.implicits._

  private val sparkContext = sparkSession.sparkContext
  private val hadoopConf = sparkContext.hadoopConfiguration

  // Input parameters.
  private val projectId = hadoopConf.get("fs.gs.project.id")
  private val bucket = hadoopConf.get("fs.gs.system.bucket")

  // Input configuration.
  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
  hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)

  def run(fullyQualifiedInputTableId: String): Unit = {
    BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)

    // Load data from BigQuery.
    val tableData: RDD[(LongWritable, JsonObject)] = sparkContext.newAPIHadoopRDD(
      hadoopConf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    val names = tableData.map({ case (key, json) =>
      if (json.has("body"))
        json.get("body").getAsString
      else
        f"no nobody for row key: $key"
    })

    val namesDf = names.toDF()
    namesDf.write.parquet("gs://cs4240-jm-parquet/names/")
  }
}
