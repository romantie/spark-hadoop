import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf().setAppName("gfteacher2").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //读取数据
    val lines = sc.textFile("F:/IDEA/maven_workplace/teacher.log")
    //整理数据
    val SubAndTeacher = lines.map(line =>{

      val teacher = line.split("/")(3)
      val sub = line.split("/")(2).split("[.]")(0)
      ((sub,teacher),1)

    })
    //聚合
    val reduce: RDD[((String, String), Int)] = SubAndTeacher.reduceByKey(_+_)
    //过滤分组排序
    val sorted = reduce.filter(_._1._1=="bigdata").sortBy(_._2)

    val fav = sorted.collect()
    println(fav.toBuffer)




  }

}
