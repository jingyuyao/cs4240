package cs4240

object Main {

  def main(args: Array[String]): Unit = {
    val op = args.head.toLowerCase
    op match {
      case "import" =>
        require(args.length > 1)
        val tables = args.slice(1, args.length)
        BigQueryImporter.run(tables)
      case "analyze" =>
        require(args.length > 0)
        CommentAnalysis.run()
      case _ => throw new IllegalArgumentException("unsupported op")
    }
  }
}
