package com.qdu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkSql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



    val csvFiles = Seq("D:/开发者/实习/SparkSQL/SparkSQL/input/hotel.csv")
    val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
    csvDF1.createOrReplaceTempView("lzny")

    csvDF1.show()
//    // 读取MySQL数据
//    val df = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test1")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "student")
//      .load()
//    df.show
//    df.createOrReplaceTempView("table1")

    var result = spark.sql(
      """
        /*1.每个月份的游客人数*/
        |   	SELECT arrival_date_month s1,SUM(adults+children+babies)s2
        |   from lzny z
        |   group by arrival_date_month
        |   order by s2
        |""".stripMargin
    )

    var result1 = spark.sql(
      """
        |/*2.每个月份的酒店平均价格*/
        |   	SELECT arrival_date_month s1,Round(avg(adr),2)s2
        |   from lzny z
        |   group by arrival_date_month
        |    order by s2
        |""".stripMargin
    )

    var result2 = spark.sql(
      """
        |/*3.酒店类型*/
        |   	SELECT hoteltype s1,count(*)s2
        |   from lzny z group by hoteltype order by s2
        |""".stripMargin
    )

    var result3 = spark.sql(
      """
      | /*4.每月价格方差*/
       SELECT arrival_date_month s1,Round(variance(abs(adr)),2) s2
        |	from lzny  group by arrival_date_month order by  s2 asc
        |""".stripMargin
    )

    var result4 = spark.sql(
      """
        /*5.统计数据中每个国家的旅游人次*/
        |	SELECT country s1,count(*) s2 from lzny z group by country order by s2 desc
        |""".stripMargin
    )
    // 保存数据
    result.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "avg_month_count")
      .mode(SaveMode.Overwrite)
      .save()


    result1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "avg_month_values")
      .mode(SaveMode.Overwrite)
      .save()
    println("已读入数据库")

    result2.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "hotel_type")
      .mode(SaveMode.Overwrite)
      .save()

    result3.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "monthly_variance")
      .mode(SaveMode.Overwrite)
      .save()

    result4.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "travel_number")
      .mode(SaveMode.Overwrite)
      .save()
    println("已读入数据库")
    // TODO 关闭环境
    spark.close()


  }
}
