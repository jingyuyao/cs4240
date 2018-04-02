package cs4240

object Main {

  def main(args: Array[String]): Unit = {
    val testTable = "fh-bigquery:reddit_comments.2008"
    BigQueryImporter.run(testTable)
//    CommentAnalysis.run(testTable)
  }
}
