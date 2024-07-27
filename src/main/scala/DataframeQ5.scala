import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DataframeQ5 {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._
    val events = List(
      (1, "2024-07-27"),
      (2, "2024-12-25"),
      (3, "2025-01-01")
    ).toDF("event_id", "date")

  val res = events.withColumn(
      "is_holiday",
      when(col("date").isin("2024-12-25", "2025-01-01"), true)
        .otherwise(false)
    )
    res.show()

  }
}
