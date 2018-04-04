package cs4240

import java.util.Properties

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryStrings, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import edu.stanford.nlp.ling.CoreAnnotations.{SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._

object BigQueryImporter {
  val commentInfoRoot = "gs://cs4240-jm-parquet/comments/"

  private val tokenProps = new Properties()
  tokenProps.setProperty("annotators", "tokenize")
  private lazy val tokenPipeline = new StanfordCoreNLP(tokenProps)

  private val sentProps = new Properties()
  sentProps.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
  sentProps.setProperty("pos.maxlen", "70")
  sentProps.setProperty("parse.maxlen", "70")
  private lazy val sentPipeline = new StanfordCoreNLP(sentProps)

  def commentInfoLocation(fullyQualifiedInputTableId: String): String =
    f"$commentInfoRoot${BigQueryStrings.parseTableReference(fullyQualifiedInputTableId).getTableId}/"

  def run(fullyQualifiedInputTableIds: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.appName("cs4240-bigquery-importer").getOrCreate
    import sparkSession.implicits._

    val sparkContext = sparkSession.sparkContext
    val hadoopConf = sparkContext.hadoopConfiguration

    // Input parameters.
    val projectId = hadoopConf.get("fs.gs.project.id")
    val bucket = hadoopConf.get("fs.gs.system.bucket")

    // Input configuration.
    hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)

    fullyQualifiedInputTableIds.foreach(fullyQualifiedInputTableId => {
      BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)

      // Load data from BigQuery.
      val tableData: RDD[(LongWritable, JsonObject)] = sparkContext.newAPIHadoopRDD(
        hadoopConf,
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[JsonObject])

      val commentInfo = tableData.flatMap({ case (_, json) => rawJsonToCommentInfo(json) })

      commentInfo.toDF.write.mode(SaveMode.Overwrite).parquet(commentInfoLocation(fullyQualifiedInputTableId))
    })

    sparkSession.stop()
  }

  private def rawJsonToCommentInfo(json: JsonObject): Option[CommentInfo] = {
    val subreddit = json.get("subreddit").getAsString.toLowerCase
    if (Data.subreddits.contains(subreddit)) {
      val body = json.get("body").getAsString
      bodyToKeywordList(body).map(
        keywordList =>
          CommentInfo(
            subreddit = subreddit,
            author = json.get("author").getAsString,
            createdTimestamp = json.get("created_utc").getAsLong,
            score = json.get("score").getAsLong,
            timesGilded = json.get("gilded").getAsLong,
            keywordList = keywordList,
            sentiment = bodyToSentiment(body)
          ))
    } else {
      None
    }
  }

  /** Returns a comma separated string of keywords, if any. */
  private def bodyToKeywordList(body: String): Option[String] = {
    val annotation = tokenPipeline.process(body)
    val words = annotation.get(classOf[TokensAnnotation]).asScala.map(_.word.toLowerCase)
    val keyWords = words.filter(Data.languages.contains)
    if (keyWords.nonEmpty)
      Some(keyWords.mkString(","))
    else
      None
  }

  /** Returns a comma separated string of sentiment values. */
  private def bodyToSentiment(body: String): String = {
    val annotation = sentPipeline.process(body)
    val sentences = annotation.get(classOf[SentencesAnnotation]).asScala
    val sentiments =
      sentences
        .map(sentence => sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]))
        .map(RNNCoreAnnotations.getPredictedClass)
    sentiments.mkString(",")
  }
}
