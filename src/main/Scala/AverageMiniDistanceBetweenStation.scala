import org.apache.spark.sql.SparkSession
import CommonFunctions.getDistance

import scala.collection.mutable.ListBuffer
object AverageMiniDistanceBetweenStation {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("AverageMiniDistanceBetweenStation").getOrCreate()
        val sc = spark.sparkContext

        // 读取站点信息 : 1,机场东,22.647011,113.8226476,1268036000,268
        val stationInfo = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt").map(line => {
            val fields = line.split(",")
            val num = fields.head.toInt - 1
            val name = fields(1)
            val lon = fields(3).toDouble
            val lat = fields(2).toDouble
            (name, num, lon, lat)
        }).cache()

        val stationMap = stationInfo.map(x => (x._1, (x._3, x._4))).collect().toList
        val allStationPair = stationInfo.flatMap(x => {
            val pairs = new ListBuffer[(String, String, Double, Double, Double, Double)]
            for (s <- stationMap) {
                if (x._1 != s._1)
                    pairs.append((x._1, s._1, x._3, x._4, s._2._1, s._2._2))
            }
            for (p <- pairs.toList) yield
                p
        })

        val distance = allStationPair.map(x => {
            val dist = getDistance(x._3, x._4, x._5, x._6)
            (x._1, (x._2, dist))
        })

        val miniDistance = distance.groupByKey().mapValues(_.toList.minBy(_._2)).map(x => (x._1, x._2._1, x._2._2)).cache()

        miniDistance.repartition(1).sortBy(_._3).saveAsTextFile(args(0) + "/liutao/RCB/MiniDistance")

        // 计算平均最短距离
        val miniDistList = miniDistance.collect().map(_._3)

        println()
        println("Average mini distance between stations is : " + (miniDistList.sum / miniDistList.length).toString + "m")
        println()

        sc.stop()

    }
}
