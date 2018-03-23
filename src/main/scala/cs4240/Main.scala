package cs4240

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    @transient lazy val sparkSession =
      SparkSession
        .builder()
        .appName("cs4240")
        .config("spark.master", "local")
        .getOrCreate()
    @transient lazy val sparkContext = sparkSession.sparkContext
    @transient lazy val conf = sparkContext.hadoopConfiguration
    import sparkSession.implicits._

    // Input parameters.
    val fullyQualifiedInputTableId = "fh-bigquery:reddit_comments.2005"
    val projectId = conf.get("fs.gs.project.id")
    val bucket = conf.get("fs.gs.system.bucket")

    // Input configuration.
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

    // Load data from BigQuery.
    val tableData: RDD[(LongWritable, JsonObject)] = sparkContext.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    val names = tableData.map({ case (key, json) =>
      if (json.has("body"))
        json.get("body").getAsString
      else
        f"no nobody for row key: $key"
    })

    // Display 10 results.
    names.take(10).foreach(obj => println(obj))

    val namesDf = names.toDF()
    namesDf.write.parquet("gs://cs4240-jm-parquet/names/")

    sparkSession.stop()
  }
}
