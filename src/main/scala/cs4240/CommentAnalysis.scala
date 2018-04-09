package cs4240

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CommentAnalysis {

  def run(): Unit = {
    val sparkSession = SparkSession.builder.appName("cs4240-comment-analysis").getOrCreate
    import sparkSession.implicits._

    println(f"Analyzing ${BigQueryImporter.commentInfoRoot} ...")

    // Pointing to a parent folder gets all the parquet files in that folder
    val commentInfo = sparkSession.read.parquet(BigQueryImporter.commentInfoRoot + "*").as[CommentInfo]

    //TODO decide what to persist

    lazy val langScoresInfo: RDD[LanguageUsageInfo] = commentInfo.rdd.flatMap(info => {
      val avgSentiScore = if (info.sentiment.nonEmpty) info.sentiment.split(",").map(_.toInt).sum / info.sentiment.length
      else 2 //Neutral
      info.keywordList.split(",").map(word => {
        LanguageUsageInfo(
          language = word,
          subreddit = info.subreddit,
          author = info.author,
          createdTimestamp = info.createdTimestamp,
          timesGilded = info.timesGilded,
          score = info.score,
          sentiment = avgSentiScore
        )
      })
    })

    /**
      * (Language (Avg Score, Avg Gildings, Avg Senti))
      */
    lazy val langOverall: RDD[(String, (Double, Double, Double))] = {
      langScoresInfo.map(usage => {
        (usage.language, (usage.score, usage.timesGilded, usage.sentiment, 1))
      }).reduceByKey({ case ((scoreA, gildA, sentiA, countA), (scoreB, gildB, sentiB, countB)) =>
        (scoreA + scoreB,
          gildA + gildB,
          sentiA + sentiB,
          countA + countB
        )
      }).mapValues({ case (score, gild, senti, count) =>
        (1.0 * score / count, 1.0 * gild / count, 1.0 * senti / count)
      })
    }

    /**
      * ((Langauge, Subreddit), (Avg Score, Avg Gildings, Avg Sentiment))
      */
    lazy val subLangPairs: RDD[((String, String), (Double, Double, Double))] = {
      langScoresInfo.map(usage => {
        ((usage.language, usage.subreddit), (usage.score, usage.timesGilded, usage.sentiment, 1))
      }).reduceByKey({ case ((scoreA, gildA, sentiA, countA), (scoreB, gildB, sentiB, countB)) =>
        (scoreA + scoreB,
          gildA + gildB,
          sentiA + sentiB,
          countA + countB
        )
      }).mapValues({ case (score, gild, senti, count) =>
        (1.0 * score / count, 1.0 * gild / count, 1.0 * senti / count)
      })
    }

    println("Sample:")
    commentInfo.take(20).foreach(println)

    println(f"Count: ${commentInfo.count()}")

    //Map Value first?
    //Print top 10 languages by overall sentiment
    langOverall.sortBy(_._2._3, ascending = false).take(10).map({ case (lang, avgs) =>
      (lang, avgs._3)
    }).foreach({ case (lang, sentiAvg) => println(f"$lang $sentiAvg") })

    sparkSession.stop()
  }
}
