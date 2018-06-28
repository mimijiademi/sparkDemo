package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MiYang on 2018/6/15 13:57.
  */
object CdnStatics {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CdnStatics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("F:\\amiyang\\BigData\\10_大数据技术之spark\\Spark教程\\2.code\\spark\\sparkCore\\sparkcore_cdn\\src\\main\\resources\\cdn.txt").cache()
    val inputArray = input.map(x => x.split(" "))

    //计算每一个IP的访问次数
    val inputSingle = inputArray.map(x => (x(0),1))
    val res=inputSingle.reduceByKey(_+_)
//    res.foreach(println(_))

    //统计每个视频访问的IP数
    val MP4_ID2Count = inputArray.filter(x => x(6).endsWith("mp4")).map(x => (x(6)+"_"+x(0),1))
    val MP42Sum = MP4_ID2Count.distinct().map{x =>
      val param = x._1.split("_")
      (param(0),x._2)
    }
    val MP4_ID2Sum = MP42Sum.reduceByKey(_+_)
//    MP4_ID2Sum.foreach(println(_))


    //统计每小时CDN的流量   时间x(3);流量x(9)
//    val HourFlow2Count = inputArray.map(x => (x(3).substring(1,14)+"_"+x(9),1))
    val HourFlow2Count = inputArray.map(x => (x(3).substring(1,15),x(9).toLong))
    val HourFlow2Sum = HourFlow2Count.reduceByKey(_+_)
    HourFlow2Sum.foreach(println(_))

    sc.stop()
  }
}
