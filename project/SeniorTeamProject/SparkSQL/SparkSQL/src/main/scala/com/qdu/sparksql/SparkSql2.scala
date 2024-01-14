package com.qdu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



    val csvFiles = Seq("C:/Users/86155/Desktop/SparkSQL/input/data.csv")
    val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
    csvDF1.createOrReplaceTempView("jjkk")

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
      |/*1.部门员工平均满意度*/
      |	SELECT division, round(AVG(satisfaction_level),3) from jjkk j group by division
        |
        |""".stripMargin
    )

    var result1 = spark.sql(
      """
        /*2.每项目部门每月平均工作时间*/
        |	SELECT number_project, AVG(average_monthly_hours+0)
        |  from jjkk j group by number_project
        |""".stripMargin
    )

    var result2 = spark.sql(
      """
        /*3.工资等级分布占比*/
        |	SELECT salary, count(salary) from jjkk j group by salary
        |""".stripMargin
    )

    var result3 = spark.sql(
      """
        /*4.每天工作量（工作量为x的人数）*/
        |	SELECT time_spend_company, count(time_spend_company)
        | from jjkk j group by time_spend_company
        |""".stripMargin
    )

    var result4 = spark.sql(
      """
        /*5.各部门对公司的评估方差*/
        |	SELECT division s1,round(variance(last_evaluation),4) as s2
        |	from jjkk  group by division order by  s1 asc
        |""".stripMargin
    )
    // 保存数据
    result.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "satisfaction_level")
      .mode(SaveMode.Overwrite)
      .save()


    result1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "avg_worktime")
      .mode(SaveMode.Overwrite)
      .save()


    result2.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "salary_level")
      .mode(SaveMode.Overwrite)
      .save()

    result3.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "daily_work")
      .mode(SaveMode.Overwrite)
      .save()

    result4.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "satisfaction_variance")
      .mode(SaveMode.Overwrite)
      .save()
    println("已读入数据库")
    // TODO 关闭环境
    spark.close()


  }
}
