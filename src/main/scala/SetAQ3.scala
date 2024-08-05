import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object SetAQ3 {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val employeeData = List(
      (1, "John", 28, 60000),
      (2, "Jane", 32, 75000),
      (3, "Mike", 45, 120000),
      (4, "Alice", 55, 90000),
      (5, "Steve", 62, 110000),
      (6, "Claire", 40, 40000)
    ).toDF("employee_id", "name", "age", "salary")

    val df1=employeeData.withColumn("age_group",when(col("age")<30,"Young")
      .when(col("age")<=30 && col("age")<=50,"Mid'")
      .otherwise("Senior"))
      .withColumn("salary_range",when(col("salary")>100000,"High")
        .when(col("salary")<50000 && col("salary")<100000,"Medium")
        .otherwise("Low"))
    df1.show()

    val df2=employeeData.filter(col("name").startsWith("J"))
    df2.show()

    val df3=employeeData.filter(col("name").endsWith("e"))
    df3.show()

    df1.groupBy("age_group").agg(
      sum("age").alias("total"),
      avg("age").alias("average_age"),
      max("age").alias("max_age"),
      min("age").alias("min_age")
    ).show()
  }

}
