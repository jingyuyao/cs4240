package cs4240

object Main {

  // fh-bigquery:reddit_comments.2009
  def main(args: Array[String]): Unit = {
    require(args.length == 2)
    val op = args.head.toLowerCase
    val table = args(1)
    println(f"Running $op on $table")
    op match {
      case "import" => BigQueryImporter.run(table)
      case "analyze" => CommentAnalysis.run(table)
      case _ => throw new IllegalArgumentException("unsupported op")
    }
  }
}
