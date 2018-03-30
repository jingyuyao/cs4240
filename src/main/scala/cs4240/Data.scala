package cs4240

object Data {
  val languages: Set[String] = Set(
    // jvm
    "java",
    "scala",
    "kotlin",
    "clojure",
    "groovy",
    // high level
    "ruby",
    "python",
    // data science
    "r",
    "julia",
    "matlab",
    // c family
    "c",
    "c++",
    "c#",
    "objective-c",
    "visual basic",
    // next gen
    "rust",
    "go",
    "swift",
    // web
    "html",
    "css",
    "javascript",
    "js",
    "typescript",
    "ts",
    "php",
    "elixir",
    "erlang",
    // scripting
    "lua",
    "bash",
    "perl",
    // functional
    "haskell",
    "lisp",
    "scheme",
    "racket",
    // old school
    "cobol",
    "fortran",
    "ada",
    "assembly",
    // etc
    "sql",
    "scratch"
  )
}

/** Parsed data from BigQuery that is stored in Parquet. */
case class CommentInfo(subreddit: String, // subreddit
                       author: String, // author
                       createdTimestamp: Long, // created_utc
                       score: Long, // score
                       timesGilded: Long, // gilded
                       keywordList: String, // body
                       sentiment: Int // body
                      )