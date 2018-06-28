package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.tools.nsc.io.Socket

/**
  * Created by MiYang on 2018/6/16 15:48.
  */
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //接收器启动的时候
  override def onStart(): Unit = {
    new Thread("socket Receiver") {
      override def run: Unit = {
        receive()
      }

    }
  }

  def receive(): Unit = {
    var socket: Socket = null
    var input: String = null

    try {
      socket = new Socket(host, port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8))


    } catch {
      case e: java.net.ConnectException =>
    }

  }

  //结束的时候,主要做资源的销毁
  override def onStop(): Unit = {

  }
}

object CustomerReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    val ssc = new StreamingContext(conf,Seconds(5))

    val linesDStream = ssc.socketTextStream("hadoop102",9999)

    val wordsDStream = linesDStream.flatMap(_.split(" "))

    val kvDStream = wordsDStream.map((_,1))

    val result = kvDStream.reduceByKey(_+_)


    result.print()

    ssc.start()

    ssc.awaitTermination()




  }

}
