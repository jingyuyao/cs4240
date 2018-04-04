package cs4240

object Main {

  def main(args: Array[String]): Unit = {
    require(args.length > 1)
    val op = args.head.toLowerCase
    val tables = args.slice(1, args.length)
    println(f"Running $op on ${tables.mkString(" ")} ...")
    op match {
      case "import" => BigQueryImporter.run(tables)
      case "analyze" => CommentAnalysis.run(tables)
      case _ => throw new IllegalArgumentException("unsupported op")
    }
  }
}
