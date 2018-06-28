import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MiYang on 2018/6/13 12:10.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建sparkConf对象，指定setAppName和master
    val conf = new SparkConf().setAppName("WordCount")//打包时使用
//    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")//IDEA本地测试时使用，local[*]代表多线程
    //通过sparkconf对象来创建sparkContext对象【该对象是你的程序和spark交互的桥梁】
    val sc = new SparkContext(conf)

    //读取数据.按空格切分 . 转换为kv结构 . 将相同key的合并
    val result = sc.textFile("./RELEASE").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_,1)
    //输出结果
    result.collect().foreach(println _)
    //关闭连接
    sc.stop()

    val a= Array(123,Array(5,7,9))


  }

}
