import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object SetAQ4 {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val moviesData = List(
      (1, "The Matrix", 9, 136),
      (2, "Inception", 8, 148),
      (3, "The Godfather", 9, 175),
      (4, "Toy Story", 7, 81),
      (5, "The Shawshank Redemption", 10, 142),
      (6, "The Silence of the Lambs", 8, 118)
    ).toDF("movie_id", "movie_name", "rating", "duration_minutes")

    val df1= moviesData.withColumn("rating_category",when(col("rating")>=8,"Excellent")
      .when(col("rating")<=6 && col("rating")<8,"Good")
      .otherwise("average"))

    df1.show()

    val df2=moviesData.withColumn("duration_minutes",when(col("duration_minutes")>150,"Long")
    .when(col("duration_minutes")>90 && col("duration_minutes")<=150,"Medium")
    .otherwise("Short"))
    df2.show()

    val df3=moviesData.filter(col("movie_name").startsWith("T")).show()
    val df4=moviesData.filter(col("movie_name").endsWith("e")).show()

    df1.groupBy("rating_category").agg(
      sum("duration_minutes").alias("total_duration_minutes"),
      avg("duration_minutes").alias("average_duration_minutes"),
      max("duration_minutes").alias("max_duration_minutes"),
      min("duration_minutes").alias("min_duration_minutes")
    ).show()


  }

}
