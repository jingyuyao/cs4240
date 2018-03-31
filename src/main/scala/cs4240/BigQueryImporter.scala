package cs4240

import java.util.Properties

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryStrings, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import edu.stanford.nlp.ling.CoreAnnotations.{SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object BigQueryImporter {
  private val sparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("cs4240-importer")
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

  val nlpProps = new Properties()
  nlpProps.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment")
  val nlpPipeline = new StanfordCoreNLP(nlpProps)

  def commentInfoLocation(fullyQualifiedInputTableId: String): String =
    f"gs://cs4240-jm-parquet/comments/${BigQueryStrings.parseTableReference(fullyQualifiedInputTableId).getTableId}/"

  def run(fullyQualifiedInputTableId: String): Unit = {
    BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)

    // Load data from BigQuery.
    val tableData: RDD[(LongWritable, JsonObject)] = sparkContext.newAPIHadoopRDD(
      hadoopConf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    val commentInfo = tableData.flatMap({ case (_, json) => rawJsonToCommentInfo(json) })
    val commentInfoDF = commentInfo.toDF
    commentInfoDF.write.parquet(commentInfoLocation(fullyQualifiedInputTableId))
  }

  def rawJsonToCommentInfo(json: JsonObject): Option[CommentInfo] = {
    val body = json.get("body").getAsString
    val annotation: Annotation = nlpPipeline.process(body)
    bodyToKeywordList(annotation) match {
      case Some(keywordList) =>
        Some(CommentInfo(
          subreddit = json.get("subreddit").getAsString,
          author = json.get("author").getAsString,
          createdTimestamp = json.get("created_utc").getAsLong,
          score = json.get("score").getAsLong,
          timesGilded = json.get("gilded").getAsLong,
          keywordList = keywordList,
          sentiment = bodyToSentiment(annotation)
        ))
      case None => None
    }
  }

  def bodyToKeywordList(annotation: Annotation): Option[String] = {
    val words = annotation.get(classOf[TokensAnnotation]).asScala
    val keyWords = words.filter(label => Data.languages.contains(label.originalText().toLowerCase))
    if (keyWords.nonEmpty)
      Some(keyWords.mkString(","))
    else
      None
  }

  def bodyToSentiment(annotation: Annotation): String = {
    val sentences = annotation.get(classOf[SentencesAnnotation]).asScala
    val sentiments = sentences.map(sentence => sentence.get(classOf[SentimentCoreAnnotations.SentimentClass]))
    sentiments.mkString(",")
  }
}
