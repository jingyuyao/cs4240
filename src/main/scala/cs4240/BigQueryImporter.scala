package cs4240

import java.util.Properties

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryStrings, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import edu.stanford.nlp.ling.CoreAnnotations.{SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._

object BigQueryImporter {
  val commentInfoRoot = "gs://cs4240-jm-parquet/comments/"

  private val nlpProps = new Properties()
  nlpProps.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
  nlpProps.setProperty("pos.maxlen", "70")
  nlpProps.setProperty("parse.maxlen", "70")
  private val nlpPipeline = new StanfordCoreNLP(nlpProps)

  def commentInfoLocation(fullyQualifiedInputTableId: String): String =
    f"$commentInfoRoot${BigQueryStrings.parseTableReference(fullyQualifiedInputTableId).getTableId}/"

  def run(fullyQualifiedInputTableId: String): Unit = {
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
    BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)

    // Load data from BigQuery.
    val tableData: RDD[(LongWritable, JsonObject)] = sparkContext.newAPIHadoopRDD(
      hadoopConf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    val commentInfo = tableData.flatMap({ case (_, json) => rawJsonToCommentInfo(json) })

    commentInfo.toDF.write.mode(SaveMode.Overwrite).parquet(commentInfoLocation(fullyQualifiedInputTableId))

    sparkSession.stop()
  }

  def rawJsonToCommentInfo(json: JsonObject): Option[CommentInfo] = {
    val subreddit = json.get("subreddit").getAsString.toLowerCase
    if (Data.subreddits.contains(subreddit)) {
      val annotation = nlpPipeline.process(json.get("body").getAsString)
      annotationToKeywordList(annotation).map(
        keywordList =>
          CommentInfo(
            subreddit = subreddit,
            author = json.get("author").getAsString,
            createdTimestamp = json.get("created_utc").getAsLong,
            score = json.get("score").getAsLong,
            timesGilded = json.get("gilded").getAsLong,
            keywordList = keywordList,
            sentiment = annotationToSentiment(annotation)
          ))
    } else {
      None
    }
  }

  def annotationToKeywordList(annotation: Annotation): Option[String] = {
    val words = annotation.get(classOf[TokensAnnotation]).asScala.map(_.word.toLowerCase)
    val keyWords = words.filter(Data.languages.contains)
    if (keyWords.nonEmpty)
      Some(keyWords.mkString(","))
    else
      None
  }

  def annotationToSentiment(annotation: Annotation): String = {
    val sentences = annotation.get(classOf[SentencesAnnotation]).asScala
    val sentiments =
      sentences
        .map(sentence => sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]))
        .map(RNNCoreAnnotations.getPredictedClass)
    sentiments.mkString(",")
  }
}
