import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DataframecomplexQ1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    employees.withColumn("category ",when(col("age") < 30 && col("salary") < 35000, "Young & Low Salary")
      .when((col("age") > 30 && col("age") < 40) && (col("salary") > 35000 && col("salary") < 45000), "Middle Aged & Medium Salary")
      .otherwise("Old & High Salary")).show()
  }

}
