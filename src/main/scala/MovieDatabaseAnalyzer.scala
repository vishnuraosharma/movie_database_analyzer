import org.apache.spark
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.util.Try

/**
 * @author Vishnu Rao-Sharma
 */
object MovieDatabaseAnalyzer extends App {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieDatabaseAnalyzer")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val path = getClass.getResource("/movie_metadata.csv").getPath
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
  df.printSchema()

  def calculateMeanRating(movieDF: DataFrame): Double = {
    movieDF.select("imdb_score")
      .agg(avg("imdb_score"))
      .first()
      .getDouble(0)
  }

  println(calculateMeanRating(df))
  println("The mean IMDB score")
  def calculateRatingStdDev(movieDF: DataFrame): Double = {
    movieDF.select("imdb_score")
      .agg(stddev("imdb_score"))
      .first()
      .getDouble(0)
  }

  println(calculateRatingStdDev(df))
  println("Calculating the standard deviation of the IMDB scores")


}