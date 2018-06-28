package com.atguigu.practice

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by MiYang on 2018/6/15 8:32.
  */
object practice {

  def getHour(timelong: String): String = {
    val datetime = new DateTime(timelong.toLong)
    datetime.getHourOfDay.toString
  }


  def main(args: Array[String]): Unit = {
    //创建sparkConf对象
    val conf = new SparkConf().setAppName("practice").setMaster("local[*]")
    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //需求1：统计每一个省份点击TOP3的广告ID

    //读取数据 RDD[String]G:\develop\IntelliJ IDEA 2016.2.4\workspace\sparkDemo\agent.log
    val logs = sc.textFile("G:\\develop\\IntelliJ IDEA 2016.2.4\\workspace\\sparkDemo\\agent.log")
    //将RDD中的String转换为数组RDD[Array[String]]   x代表每一行，=> 代表匿名函数
    val logsArray = logs.map(x => x.split(" "))

/*

    //提取相应的数据，转换粒度 RDD[( pro_adid, 1 )]
    val proAndAd2Count = logsArray.map(x => (x(1) + "_" + x(4), 1))
    //将每一个省份每一个广告的所有点击量聚合 RDD[( pro_adid, sum )]
    val proAndAd2Sum = proAndAd2Count.reduceByKey((x, y) => x + y)
    //将粒度扩大， 拆分key， RDD[ ( pro, (sum, adid) ) ]
    val pro2AdSum = proAndAd2Sum.map{ x => val param = x._1.split("_"); ( param(0), (param(1), x._2))}
    //将每一个省份所有的广告合并成一个数组 RDD[ (pro, Array[ (adid, sum) ]) ]
    val pro2AdArray = pro2AdSum.groupByKey()
    //排序取前3                                                sortWith(lt: (A, A) => Boolean)
    val result = pro2AdArray.mapValues(values => values.toList.sortWith((x, y) => x._2 > y._2).take(3))
    //行动操作
    result.collectAsMap()
*/

    //需求2：统计每一个省份每一个小时的TOP3广告的ID     map(key : map( k: Array(Int)))
    //产生最小粒度  RDD[ ( pro_hour_ad , 1 ) ]
    val pro_hour_ad2Count = logsArray.map {
      x => (x(1) + "_" + x(0) + "_" + x(4), 1)
    }
    //计算每一个省份每一个小时每一个广告的点击总量  RDD[ ( pro_hour_ad , sum ) ]
    val pro_hour_ad2Sum = pro_hour_ad2Count.reduceByKey(_ + _)
    //拆分key，扩大粒度  RDD[ ( pro_hour , (ad, sum) ) ]
    val pro_hour2AdSum = pro_hour_ad2Sum.map { x =>
      val param = x._1.split("_");
      (param(0) + "_" + param(1), (param(2), x._2))
    }
    //将一个省份一个小时内的数据聚合 RDD[ ( pro_hour , Array[ (ad, sum) ] ) ]
    val pro_hour2AdGroup = pro_hour2AdSum.groupByKey()

    //直接对一个小时内的广告排序，取前三
    val pro_hour2Top3Array = pro_hour2AdGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }
    //扩大粒度， RDD[ ( pro , (hour, Array[(ad, sum)])  ) ]
    val pro2hourAdArray = pro_hour2Top3Array.map{x =>
      val param = x._1.split("_")
      (param(0),(param(1),x._2))
    }
    //将每个省份的  一个小时内的广告top3  求出
    val result2 = pro2hourAdArray.groupByKey()

    //得到最终结果
    result2.collectAsMap().foreach(println(_))


    //关闭SparkContext
    sc.stop()

  }

}
