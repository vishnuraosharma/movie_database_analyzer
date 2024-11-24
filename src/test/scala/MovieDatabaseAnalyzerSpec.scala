import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import MovieDatabaseAnalyzer.calculateMeanRating
import MovieDatabaseAnalyzer.calculateRatingStdDev

class MovieDatabaseAnalyzerSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieDatabaseAnalyzer")
    .master("local[*]")
    .getOrCreate()

  val path = getClass.getResource("/movie_metadata.csv").getPath
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  behavior of "MovieDatabaseAnalyzer"

  it should "calculateMeanRating within +/- 0.1" in {
    val meanRating = calculateMeanRating(df)
    // 6.453200745804848 is the mean rating of the dataset
    meanRating shouldBe 6.44 +- 0.1
  }
  it should "calculateRatingStdDev within +/- 0.1" in {
    val ratingStdDev = calculateRatingStdDev(df)
    // 0.9988071293753289 is the standard deviation of the dataset
    ratingStdDev shouldBe 0.9988071293753289 +- 0.1
  }
}