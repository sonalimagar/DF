import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum,min,max, when}

object SetAQ2 {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val data = List(
      (1, "Smartphone", 700, "Electronics"),
      (2, "TV", 1200, "Electronics"),
      (3, "Shoes", 150, "Apparel"),
      (4, "Socks", 25, "Apparel"),
      (5, "Laptop", 800, "Electronics"),
      (6, "Jacket", 200, "Apparel")
    ).toDF("product_id", "product_name", "price", "category")

    val df1= data.withColumn("price_category",when(col("price")>500,"Expensive")
      .when(col("price")<=200 && col("price")<500,"Moderate")
      .otherwise("Cheap"))
     df1.show()

    val df2= data.filter(col("product_name").startsWith("S"))
    df2.show()

      val df3=data.filter(col("product_name").endsWith("s"))
      df3.show()

  df1.groupBy("price_category").agg(
      sum("price").alias("total_price"),
      avg("price").alias("average_price"),
      max("price").alias("max_price"),
      min("price").alias("min_price")
    ).show()


  }

}
