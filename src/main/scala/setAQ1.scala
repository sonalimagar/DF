import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, when,min,max}

object setAQ1 {
  def main(args:Array[String]):Unit={
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")
    ).toDF("student_id", "name", "score", "subject")

    val dfWithGrade = data.withColumn("grade",when(col("score")>=90,"A")
      .when(col("score")<=80 && col("score")<90,"B")
      .when(col("score")<=70 &&col("score")<80,"C")
      .when(col("score")<=60 &&col("score")<70,"D")
      .otherwise("F"))
    dfWithGrade.show()



    val avgscorepersubject = dfWithGrade.groupBy("subject")
      .agg(avg("score").alias("average_score"))
    avgscorepersubject.show()


    val minmaxscore = dfWithGrade.groupBy("subject")
      .agg(
        max("score").alias("max_score"),
        min("score").alias("min_score")
      )
    minmaxscore.show()


    val count_by_grade = dfWithGrade.groupBy("subject", "grade").count().alias("count of student per subject")
    count_by_grade.show()
  }

}
