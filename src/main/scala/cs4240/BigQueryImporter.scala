package cs4240

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryStrings, GsonBigQueryInputFormat}
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

    val commentInfo = tableData.map({ case (_, json) => rawJsonToCommentInfo(json)})
    val commentInfoDF = commentInfo.toDF

    val tableReference = BigQueryStrings.parseTableReference(fullyQualifiedInputTableId)
    commentInfoDF.write.parquet(f"gs://cs4240-jm-parquet/comments/${tableReference.getTableId}/")
  }

  def rawJsonToCommentInfo(json: JsonObject): CommentInfo =
    CommentInfo(
      subreddit = json.get("subreddit").getAsString,
      author = json.get("author").getAsString,
      createdTimestamp = json.get("created_utc").getAsLong,
      score = json.get("score").getAsLong,
      timesGilded = json.get("gilded").getAsLong,
      keywordList = json.get("body").getAsString,
      sentiment = 0
    )
}
