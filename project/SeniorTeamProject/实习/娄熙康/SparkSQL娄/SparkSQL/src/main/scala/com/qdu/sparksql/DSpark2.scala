package com.qdu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}



object DSpark2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



    val csvFiles = Seq("D:/开发者/实习/娄熙康/SparkSQL娄/SparkSQL/input/daikuan_train.csv")
    val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
    csvDF1.createOrReplaceTempView("loans")

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

//    var result1 = spark.sql(
//      """
//        |/*1.平均贷款金额，大于小于平均贷款金额的比率-------饼状图*/
//
//        |select count(*) from loans where loanAmnt >(select avg(loanAmnt) from loans)
//        |union
//        |select count(*) from loans where loanAmnt <(select avg(loanAmnt) from loans)
//        |""".stripMargin
//    )

//    var result2 = spark.sql(
//      """
//        |/*2、贷款各年限贷款总数----------条形图*/
//        |SELECT
//        |    term,
//        |    COUNT(*) as total_loans
//        |FROM loans
//        |GROUP BY term order by term desc
//        |""".stripMargin
//    )

//    var result3 = spark.sql(
//      """
//        |/*3.每一年的贷款总数的变化情况---------饼状图*/
//        |   	 SELECT left(issueDate,4) as year,sum(loanAmnt) as sum
//        |FROM loans
//        |GROUP BY year order by sum
//        |""".stripMargin
//    )

//var result4 = spark.sql(
//  """
//    |/*4、每一年的平均贷款利率变化情况--------折线图*/
//    |   	 SELECT left(issueDate,4) as year,avg(interestRate) as avgrate
//    |FROM loans
//    |GROUP BY year order by year
//    |""".stripMargin
//)

//    var result5 = spark.sql(
//      """
//        |/*5.贷款等级分布-------饼状图*/
//        |   	SELECT
//        |    grade,
//        |    COUNT(*) as count
//        |FROM loans
//        |GROUP BY grade order by count desc
//        |""".stripMargin
//    )

//    var result6 = spark.sql(
//      """
//        |/*6、年收入与贷款金额的比值，0-1,1-2,2-3,3以上分布------饼状图*/
//        |    SELECT SUM(CASE WHEN income_to_loan_ratio>0 and income_to_loan_ratio<1 THEN 1 ELSE 0 END) AS a,
//        |           SUM(CASE WHEN income_to_loan_ratio>1 and income_to_loan_ratio<2 THEN 1 ELSE 0 END) AS b,
//        |           SUM(CASE WHEN income_to_loan_ratio>2 and income_to_loan_ratio<3 THEN 1 ELSE 0 END) AS c,
//        |           SUM(CASE WHEN income_to_loan_ratio>3 THEN 1 ELSE 0 END) AS others
//        |FROM (SELECT annualIncome, loanAmnt, (annualIncome/loanAmnt) as income_to_loan_ratio FROM loans)
//        |""".stripMargin
//    )

//        var result7 = spark.sql(
//          """
//            |/* 7、信贷周转余额合计(revolBal列)各范围的分布0-5000,5000-10000,10000-30000,30000以上--------饼状图*/
//            |   	SELECT SUM(CASE WHEN revolBal>0 and revolBal<5000 THEN 1 ELSE 0 END) AS a,
//            |           SUM(CASE WHEN revolBal>5000 and revolBal<10000 THEN 1 ELSE 0 END) AS b,
//            |           SUM(CASE WHEN revolBal>10000 and revolBal<30000 THEN 1 ELSE 0 END) AS c,
//            |           SUM(CASE WHEN revolBal>30000 THEN 1 ELSE 0 END) AS others
//            |FROM  loans
//            |""".stripMargin
//        )


//var result8 = spark.sql(
//  """
//    |/* 8、年收入前十----------折线图 */
//    |   	SELECT annualIncome
//    |FROM loans group by annualIncome order by annualIncome desc limit 10
//    |""".stripMargin
//)

//var result9 = spark.sql(
//  """
//    |/* 9、在还款之前还能借的金额(totalAcc*100-revolBal)各范围的分布：0-5000,5000-10000,10000-20000,20000以上-------条形图*/
//    |   	SELECT SUM(CASE WHEN money>0 and money<5000 THEN 1 ELSE 0 END) AS a,
//    |           SUM(CASE WHEN money>5000 and money<10000 THEN 1 ELSE 0 END) AS b,
//    |           SUM(CASE WHEN money>10000 and money<20000 THEN 1 ELSE 0 END) AS c,
//    |           SUM(CASE WHEN money>20000 THEN 1 ELSE 0 END) AS others
//    |FROM  (SELECT totalAcc, revolBal, (totalAcc*100-revolBal) as money FROM loans)
//    |""".stripMargin
//)

    var result10 = spark.sql(
      """
        |/*10、银行每年所得利润(loanAmnt*interestRate)变化情况--------折线图*/
        |   	 SELECT left(issueDate,4) as year,sum(loanAmnt*interestRate) as lirun
        |FROM loans
        |GROUP BY year order by year
        |""".stripMargin
    )


    // 保存数据
//    result1.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "avg")
//      .mode(SaveMode.Overwrite)
//      .save()

//    result2.write
//          .format("jdbc")
//          .option("url", "jdbc:mysql://localhost:3306/test2")
//          .option("driver", "com.mysql.jdbc.Driver")
//          .option("user", "root")
//          .option("password", "root")
//          .option("dbtable", "task2")
//          .mode(SaveMode.Overwrite)
//          .save()

//    result3.write
//              .format("jdbc")
//              .option("url", "jdbc:mysql://localhost:3306/test2")
//              .option("driver", "com.mysql.jdbc.Driver")
//              .option("user", "root")
//              .option("password", "root")
//              .option("dbtable", "task3")
//              .mode(SaveMode.Overwrite)
//              .save()

//    result4.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "task4")
//      .mode(SaveMode.Overwrite)
//      .save()

//    result5.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "task5")
//      .mode(SaveMode.Overwrite)
//      .save()

//    result6.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "task6")
//      .mode(SaveMode.Overwrite)
//      .save()

//    result7.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "task7")
//      .mode(SaveMode.Overwrite)
//      .save()

//    result8.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "task8")
//      .mode(SaveMode.Overwrite)
//      .save()

//    result9.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test2")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "task9")
//      .mode(SaveMode.Overwrite)
//      .save()

    result10.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "task10")
      .mode(SaveMode.Overwrite)
      .save()




    println("已读入数据库")
    // TODO 关闭环境
    spark.close()


  }
}
