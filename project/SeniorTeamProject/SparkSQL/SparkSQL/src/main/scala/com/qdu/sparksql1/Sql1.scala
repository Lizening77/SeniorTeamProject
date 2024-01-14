package com.qdu.sparksql1


  import org.apache.spark.SparkConf
  import org.apache.spark.sql.{SaveMode, SparkSession}


object Sql1 {
    def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
      val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



      val csvFiles = Seq("C:/Users/86155/Desktop/SparkSQL/input/Walmart_Store_sales.csv")
      val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
      csvDF1.createOrReplaceTempView("ssmart")

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
          |/*1.每个商店的营业额之和*/
          |   	SELECT store s1,SUM(weekly_sales)s2 from ssmart s group by store
          |""".stripMargin
      )

      var result1 = spark.sql(
        """
          |   /*2.平均cpi(消费者价格指数)*/
          |  	SELECT store s1,AVG(cpi) c from ssmart s group by store


          |""".stripMargin
      )

      var result2 = spark.sql(
        """
          |	/*3.每个商店的营业额方差*/
          |	SELECT store+0 s1,variance(weekly_sales+0) vv from ssmart s
          | group by store order by  s1 asc
          |""".stripMargin
      )

      var result3 = spark.sql(
        """
          |  	/*4.假期对平均燃料费的影响*/
          | 	SELECT holiday_flag ,avg(fuel_price) as fuel_price from ssmart s
          |   group by holiday_flag
          |""".stripMargin
      )

      var result4 = spark.sql(
        """
          |	/*5.每个店的最大值最小值差*/
          | select s1,s2-s3 as difference from (
          |	SELECT store s1,max(cpi +0) as s2,MIN(cpi+0) as s3
          |	 	from ssmart s group by store order by store+0 )
          |""".stripMargin
      )
      // 保存数据
      result.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test2")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "root")
        .option("dbtable", "total_revenue")
        .mode(SaveMode.Overwrite)
        .save()


      result1.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test2")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "root")
        .option("dbtable", "average_cpi")
        .mode(SaveMode.Overwrite)
        .save()


      result2.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test2")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "root")
        .option("dbtable", "turnover_variance")
        .mode(SaveMode.Overwrite)
        .save()

      result3.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test2")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "root")
        .option("dbtable", "effect")
        .mode(SaveMode.Overwrite)
        .save()

      result4.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test2")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "root")
        .option("dbtable", "deviation")
        .mode(SaveMode.Overwrite)
        .save()
      println("已读入数据库")
      // TODO 关闭环境
      spark.close()


    }

}
