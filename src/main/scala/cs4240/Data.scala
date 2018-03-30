package cs4240

/** Parsed data from BigQuery that is stored in Parquet. */
case class CommentInfo(subreddit: String, // subreddit
                       author: String, // author
                       createdTimestamp: Long, // created_utc
                       score: Long, // score
                       timesGilded: Long, // gilded
                       keywordList: String, // body
                       sentiment: Int // body
                      )
