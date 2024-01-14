package com.qdu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}



object DSpark2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // TODO 创建SparkSQL的运行环境



    val csvFiles = Seq("D:/开发者/实习/SparkSQL/SparkSQL/input/data.csv")
    val csvDF1 = spark.read.format("csv").option("header", true).load(csvFiles.mkString(","))
    csvDF1.createOrReplaceTempView("data")

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

//    var result6 = spark.sql(
//      """
//        |/*7.各酒店入住高峰期统计*/
//        |   	select distinct hname,month,number from(
//        |           select hname,month,COUNT(month) as number from data group by hname,month order by number desc)
//        |""".stripMargin
//    )

    var result7 = spark.sql(
      """
        |/*8.酒店购票票型统计（单人，团体，公务，其他）*/
        |SELECT
        |    count(*)
        |FROM data where MODEL1 = 'Direct'
        |union
        |SELECT
        |    count(*)
        |FROM data where MODEL1 = 'Corporate'
        |union
        |SELECT
        |    count(*)
        |FROM data where MODEL1 = 'Online TA'
        |union
        |SELECT
        |    count(*)
        |FROM data where MODEL1 like 'Offline%'
        |""".stripMargin
    )

//    var result8 = spark.sql(
//      """
//        |/*9.近三年宾馆营收*/
//        |   	 select hname,year,round(sum(W),2) as money from data group by hname,year
//        |      order by year
//        |""".stripMargin
//    )

//    var result9 = spark.sql(
//            """
//              |/*10.Resort Hotel已确定的团队票清单*/
//              |   	 select V,count(*) from data group by V
//              |""".stripMargin
//          )

    // 保存数据
//    result6.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test1")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "MAX_month_count")
//      .mode(SaveMode.Overwrite)
//      .save()

    result7.write
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/test1")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "tickes_model2")
          .mode(SaveMode.Overwrite)
          .save()
//
//    result8.write
//              .format("jdbc")
//              .option("url", "jdbc:mysql://localhost:3306/test1")
//              .option("driver", "com.mysql.jdbc.Driver")
//              .option("user", "root")
//              .option("password", "root")
//              .option("dbtable", "YINCOME2")
//              .mode(SaveMode.Overwrite)
//              .save()

//    result9.write
//                  .format("jdbc")
//                  .option("url", "jdbc:mysql://localhost:3306/test1")
//                  .option("driver", "com.mysql.jdbc.Driver")
//                  .option("user", "root")
//                  .option("password", "root")
//                  .option("dbtable", "juzhubi")
//                  .mode(SaveMode.Overwrite)
//                  .save()
//    println("已读入数据库")
    // TODO 关闭环境
    spark.close()


  }
}
