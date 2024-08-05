import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, month, sum, to_date, when}

object SetAQ5 {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val transactionData = List(
      (1, "2023-12-01", 1200, "Credit"),
      (2, "2023-11-15", 600, "Debit"),
      (3, "2023-12-20", 300, "Credit"),
      (4, "2023-10-10", 1500, "Debit"),
      (5, "2023-12-30", 250, "Credit"),
      (6, "2023-09-25", 700, "Debit")
    ).toDF("transaction_id", "transaction_date", "amount", "transaction_type")

    val df1=transactionData.withColumn("amount_category",when(col("amount")>1000,"High")
    .when(col("amount")<=500 && col("amount")<=1000,"Medium")
    .otherwise("Low"))

    df1.show()

    val df2=transactionData.withColumn("transaction_month ",month(col("transaction_date"))).show()

     println("transactions that occurred in the month of 'December' is below")
    val transactionDFWithDate = df1.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    val decemberTransactions = transactionDFWithDate.filter(month(col("transaction_date")) === 12)
    decemberTransactions.show()

    println("Summary of transection")
    val df4=df1.groupBy("transaction_type").agg(
      sum("amount").alias("total_amount"),
      avg("amount").alias("average_amount"),
      max("amount").alias("max_amount"),
      min("amount").alias("min_amount")
    )
      df4.show()

  }

}
