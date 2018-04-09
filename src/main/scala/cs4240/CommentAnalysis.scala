package cs4240

import java.time.{Instant, YearMonth}

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
    lazy val subLangPairs: RDD[((String, String), (Double, Double, Double, Int))] = {
      langScoresInfo.map(usage => {
        ((usage.language, usage.subreddit), (usage.score, usage.timesGilded, usage.sentiment, 1))
      }).reduceByKey({ case ((scoreA, gildA, sentiA, countA), (scoreB, gildB, sentiB, countB)) =>
        (scoreA + scoreB,
          gildA + gildB,
          sentiA + sentiB,
          countA + countB
        )
      }).mapValues({ case (score, gild, senti, count) =>
        (1.0 * score / count, 1.0 * gild / count, 1.0 * senti / count, count)
      })
    }

    lazy val topLanguages =
      langOverall
        .sortBy(_._2._3, ascending = false)
        .map({ case (lang, avgs) =>
          (lang, avgs._3)
        })

    //    topLanguages.take(10).foreach({ case (lang, sentiAvg) => println(f"$lang $sentiAvg") })

    lazy val mostHatedPerSub: RDD[(String, (String, Double))] =
      subLangPairs
        .filter(_._2._4 >= 40)
        .map({ case (langSub, scores) => (langSub._2, (langSub._1, scores._3)) })
        .reduceByKey((l, r) => if (l._2 < r._2) l else r)
        .sortBy(_._1)

    //    mostHatedPerSub.collect().foreach(println)

    lazy val mostLovePerSub: RDD[(String, (String, Double))] =
      subLangPairs
        .filter(_._2._4 >= 40)
        .map({ case (langSub, scores) => (langSub._2, (langSub._1, scores._3)) })
        .reduceByKey((l, r) => if (l._2 > r._2) l else r)
        .sortBy(_._1)

//    mostLovePerSub.collect().foreach(println)

    lazy val classicJvC =
      subLangPairs
        .filter(d => (d._1._2 == "java" && d._1._1 == "c++") || (d._1._2 == "cpp" && d._1._1 == "java"))

    classicJvC.collect().foreach(println)

    val oldLanguages = Set("cobol", "fortran", "ada", "assembly")
    val stdLanguages = Set("c", "c++", "java", "python")
    val newLanguages = Set("scala", "go", "swift", "kotlin", "rust")
    lazy val oldSentOvertime = timeSentOfSet(oldLanguages)
    lazy val stdSentOvertime = timeSentOfSet(stdLanguages)
    lazy val newSentOvertime = timeSentOfSet(newLanguages)

    //(Language, YYYY-mm , avg senti)
    def timeSentOfSet(set: Set[String]): RDD[(String, String, Double)] = {
      langScoresInfo
        .filter(l => set.contains(l.language))
        .map(l => {
          val created = Instant.ofEpochMilli(l.createdTimestamp)
          val yearMonth = YearMonth.from(created)
          ((l.language, yearMonth.getYear, yearMonth.getMonthValue), (l.sentiment, 1))
        }).reduceByKey((l, r) => {
        (l._1 + r._1, l._2 + r._2)
      }).map({case ((l, y, m), (s, t)) => (l, y + "-" + "%02d".format(m), 1.0 * s / t)})
    }

    sparkSession.stop()
  }
}
