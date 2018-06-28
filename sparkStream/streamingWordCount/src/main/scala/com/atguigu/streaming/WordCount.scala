package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by MiYang on 2018/6/16 15:48.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    val ssc = new StreamingContext(conf,Seconds(5))

    val linesDSteam = ssc.socketTextStream("hadoop102",9999)

    val wordsDStream = linesDSteam.flatMap(_.split(" "))

    val kvDStream = wordsDStream.map((_,1))

    val result = kvDStream.reduceByKey(_+_)


    result.print()

    ssc.start()

    ssc.awaitTermination()




  }

}
