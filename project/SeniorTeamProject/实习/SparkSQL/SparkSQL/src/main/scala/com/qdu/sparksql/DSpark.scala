package com.qdu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object DSpark {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



    val csvFiles = Seq("D:/开发者/实习/SparkSQL/SparkSQL/input/income_census_train.csv")
    val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
    csvDF1.createOrReplaceTempView("census")

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
            /*1.年龄金字塔（0-25，26-50，51及以上）*/
            |SELECT
            |    count(*)
            |FROM census where age between 0 and 25
            |union
            |SELECT
            |    count(*)
            |FROM census where age between 26 and 60
            |union
            |SELECT
            |    count(*)
            |FROM census where age>60
            |""".stripMargin
        )

//    var result1 = spark.sql(
//      """
//        /*功能2：显示种族比例*/
//        |select race,count(*) from census group by race
//        |""".stripMargin
//    )

//        var result2 = spark.sql(
//          """
//            |/*3.人种受教育情况的比较*/
//            |   	select race,education,count(*) from census group by race,education
//            |""".stripMargin
//        )

//        var result3 = spark.sql(
//          """
//            | /*4.青年职业选择趋势*/
//            |SELECT workclass from census where age between 22 and 30
//            |group by workclass order by count(workclass) desc limit 5
//            |""".stripMargin
//        )
    //
//        var result4 = spark.sql(
//          """
//            /*5.各职业平均周工作时长及收入档次（收入高于人均收入为1，否则为0）比较*/
//            |	select workclass,round(avg(hours_per_week),2) as awtime,income_bracket,count(income_bracket)
//            |  from census group by workclass,income_bracket order by awtime
//            |""".stripMargin
//        )

//          var result5 = spark.sql(
//                """
//                  /*6.男女比例及婚姻状况调查*/
//                  |select gender,marital_status,count(*) from census group by gender,marital_status
//                  |""".stripMargin
//              )
    // 保存数据
        result.write
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/test1")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "age_pyramid2")
          .mode(SaveMode.Overwrite)
          .save()


//    result1.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test1")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "race_proportion")
//      .mode(SaveMode.Overwrite)
//      .save()
//    println("已读入数据库")

//        result2.write
//          .format("jdbc")
//          .option("url", "jdbc:mysql://localhost:3306/test1")
//          .option("driver", "com.mysql.jdbc.Driver")
//          .option("user", "root")
//          .option("password", "root")
//          .option("dbtable", "education")
//          .mode(SaveMode.Overwrite)
//          .save()
//    //
//        result3.write
//          .format("jdbc")
//          .option("url", "jdbc:mysql://localhost:3306/test1")
//          .option("driver", "com.mysql.jdbc.Driver")
//          .option("user", "root")
//          .option("password", "root")
//          .option("dbtable", "professoion_choice")
//          .mode(SaveMode.Overwrite)
//          .save()
    //
//        result4.write
//          .format("jdbc")
//          .option("url", "jdbc:mysql://localhost:3306/test1")
//          .option("driver", "com.mysql.jdbc.Driver")
//          .option("user", "root")
//          .option("password", "root")
//          .option("dbtable", "wt_income")
//          .mode(SaveMode.Overwrite)
//          .save()

//           result5.write
//              .format("jdbc")
//              .option("url", "jdbc:mysql://localhost:3306/test1")
//              .option("driver", "com.mysql.jdbc.Driver")
//              .option("user", "root")
//              .option("password", "root")
//              .option("dbtable", "mapo")
//              .mode(SaveMode.Overwrite)
//              .save()
    println("已读入数据库")
    // TODO 关闭环境
    spark.close()


  }
}
