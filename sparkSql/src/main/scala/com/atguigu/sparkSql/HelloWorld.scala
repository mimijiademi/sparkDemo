package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by MiYang on 2018/6/15 16:23.
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val spark1 = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark1.sparkContext

    val df = spark1.read.json("F:\\amiyang\\BigData\\10_大数据技术之spark\\Spark教程\\2.code\\spark\\sparkSql\\doc\\employees.json")
    //注意，导入隐式转换，
    import spark1.implicits._
    //展示整个表
    df.show()
    //展示整个表的Scheam
    df.printSchema()
    //DSL风格查询,一定要导入隐式转换
    df.filter($"salary">3500).show

    //SQL风格

    //首先注册一个表名
    df.createOrReplaceTempView("employee")
    //查询
    spark1.sql("select * from employee where salary> 3300").show()

    spark1.close()

  }

}
