package cs4240

object Main {

  def main(args: Array[String]): Unit = {
    val testTable = "fh-bigquery:reddit_comments.2005"
//    BigQueryImporter.run(testTable)
    ParquetImporter.run(testTable).take(10).foreach(println)
  }
}
