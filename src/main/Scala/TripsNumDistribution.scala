import CommonFunctions.dayOfMonth_string
import org.apache.spark.sql.SparkSession

object TripsNumDistribution {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("TripsNumDistribution").getOrCreate()
    val sc = spark.sparkContext

    // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
    val tripsNum = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      (id, 1)
    }).reduceByKey(_+_).map(x => (x._2, 1)).reduceByKey(_+_).cache()

    val total = tripsNum.map(_._2).reduce(_+_)
    val dis = tripsNum.map(x => (x._1, x._2.toFloat / total)).repartition(1).sortByKey()
    dis.saveAsTextFile(args(0) + "/zlt/RCB/TripsNumDistribution")
    sc.stop()
  }
}
