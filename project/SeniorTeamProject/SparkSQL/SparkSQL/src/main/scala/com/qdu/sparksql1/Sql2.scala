package com.qdu.sparksql1


import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object Sql2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



    val csvFiles = Seq("C:/Users/86155/Desktop/SparkSQL/input/Global Internet users.csv")
    val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
    csvDF1.createOrReplaceTempView("lxkk")

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
        |  	/*1.每年全球网民数量*/
        |	SELECT year, sum(user_num) as suser from lxkk l group by year
        |""".stripMargin
    )

    var result1 = spark.sql(
      """
        |	 /*2.各个地区100个人订购网络数量*/
        |	SELECT code, AVG(mobile_user+0) as muser from lxkk l group by code


        |""".stripMargin
    )

    var result2 = spark.sql(
      """
        |	 /*3.中国网络普及趋势*/
        |	SELECT year,user_proportion from lxkk l where country='China'
        |""".stripMargin
    )

    var result3 = spark.sql(
      """
        |	 /*4.各地区目前的固定宽带用户数量*/
        |	SELECT code,broadband_user  from lxkk l where year='2020' limit 45
        |""".stripMargin
    )

    var result4 = spark.sql(
      """
        |	 /*5.各地区目前移动网络用户中宽带网络的普及率(单位1/1000)*/
        |  	select code,(broadband_user/year)*1000 as  peneteation_rate from lxkk l
        |   where year='2020' limit 45
        |""".stripMargin
    )
    // 保存数据
    result.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "total_netizen")
      .mode(SaveMode.Overwrite)
      .save()


    result1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "order_num")
      .mode(SaveMode.Overwrite)
      .save()


    result2.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "trend")
      .mode(SaveMode.Overwrite)
      .save()

    result3.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user_num")
      .mode(SaveMode.Overwrite)
      .save()

    result4.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "peneteation_rate")
      .mode(SaveMode.Overwrite)
      .save()
    println("已读入数据库")
    // TODO 关闭环境
    spark.close()


  }

}
