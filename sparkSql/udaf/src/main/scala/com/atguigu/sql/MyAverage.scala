package com.atguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by MiYang on 2018/6/16 10:03.
  */
class MyAverage extends UserDefinedAggregateFunction {
  //聚合函数输入的数据类型,eg : sum(score) 的score的类型
  override def inputSchema: StructType = StructType(StructField("salary", LongType) :: Nil)

  //小范围聚合临时变量的类型
  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

  //返回值的类型
  override def dataType: DataType = DoubleType

  //幂等性
  override def deterministic: Boolean = true

  //初始化你的数据结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //每一个分区中去计算，更新数据结构
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将所有分区的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算值
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble / buffer.getLong(1) //sum/count
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("udaf").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //注册
    spark.udf.register("average",new MyAverage())
    val df = spark.read.json("F:\\amiyang\\BigData\\10_大数据技术之spark\\Spark教程\\2.code\\spark\\sparkSql\\doc\\employees.json")
    df.createOrReplaceTempView("employee")
    spark.sql("select average(salary) as avg from employee").show()
    spark.stop()


  }
}