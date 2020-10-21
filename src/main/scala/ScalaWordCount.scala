import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    val conf=new SparkConf().setAppName("WordCCount").setMaster("local[2]")
    //创建spark的入kou
    val sc=new SparkContext(conf)
    //指定以后从哪里读取数据创建rdd
    val lines: RDD[String] =sc.textFile("F:/testlm.txt")
    //切分压平
    val words: RDD[String] =lines.flatMap(_.split(" "))
    //将单词和1组合

    val wordAndOne: RDD[(String, Int)] = {
      words.map((_,1))
    }
    //按key值进行聚合
    val reduce: RDD[(String, Int)] =wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] =reduce.sortBy(_._2,false)
    //将结果保存
    sorted.saveAsTextFile("F:/testreslmk")
    print(reduce.collect())
    //释放资源
    sc.stop()

  }
}
