package com.atguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by MiYang on 2018/6/16 13:57.
  */
case class tbDate(dateid:String,years:Int,theyear:Int,month:Int,day:Int,weekday:Int,week:Int,quarter:Int,period:Int,halfmonth:Int){}

case class tbStock(ordernumber:String,locationid:String,dateid:String) {}

case class tbStockDetail(ordernumber:String,rownum:Int,itemid:String,number:Int,price:Double,amount:Int) {}



object practice {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val conf = new SparkConf().setAppName("practice").setMaster("local[*]")
    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //加载数据到Hive
    val tbDateRdd = spark.sparkContext.textFile("F:\\amiyang\\BigData\\10_大数据技术之spark\\Spark教程\\2.code\\spark\\sparkSql\\doc\\tbDate.txt")
    val tbDateDS = tbDateRdd.map(_.split(",")).map(x => tbDate(x(0),x(1).trim().toInt,x(2).trim().toInt,x(3).trim().toInt,x(4).trim().toInt,x(5).trim().toInt,x(6).trim().toInt,x(7).trim().toInt,x(8).trim().toInt,x(9).trim().toInt)).toDS()
    tbDateDS.createOrReplaceTempView("tbDate")

    val tbStockRdd = spark.sparkContext.textFile("F:\\amiyang\\BigData\\10_大数据技术之spark\\Spark教程\\2.code\\spark\\sparkSql\\doc\\tbStock.txt")
    val tbStockDS = tbStockRdd.map(_.split(",")).map(x => tbStock(x(0),x(1),x(2))).toDS()
    tbStockDS.createOrReplaceTempView("tbStock")

    val tbStockDetailRdd = spark.sparkContext.textFile("F:\\amiyang\\BigData\\10_大数据技术之spark\\Spark教程\\2.code\\spark\\sparkSql\\doc\\tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(x => tbStockDetail(x(0).trim,x(1).trim.toInt,x(2).trim.toString,x(3).trim.toInt,x(4).trim.toDouble,x(5).trim.toInt)).toDS()
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    tbDateDS.show()
    tbStockDS.show()
    tbStockDetailDS.show()



  }
}

def insertHive(spark:SparkSession,){

}


