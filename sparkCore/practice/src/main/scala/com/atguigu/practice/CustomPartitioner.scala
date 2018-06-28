package com.atguigu.practice

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by MiYang on 2018/6/15 10:50.
  */
class CustomPartitioner(numPar : Int) extends Partitioner{
  override def numPartitions:Int = numPar

  override def getPartition(key: Any): Int = {
//    val ckey =
    1
  }
}

object Test{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitioner").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.makeRDD(List("aa.2","bb.2","cc.3"))

  }
}