import org.apache.spark.sql.SparkSession
import CommonFunctions.{halfHourOfMonth, transTimeToTimestamp}

import scala.collection.mutable.ListBuffer

object ODPeriodicFlow {
    def main(args: Array[String]): Unit = {
        /**
         * 统计每个OD pair之间的周期流量
         */
        val spark = SparkSession.builder().appName("ODPeriodicFlow").getOrCreate()
        val sc = spark.sparkContext

        // 读取站点信息 : 1,机场东,22.647011,113.8226476,1268036000,268
        val stationInfo = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt").map(line => {
            val fields = line.split(",")
            val num = fields.head.toInt - 1
            val name = fields(1)
            val lon = fields(3).toDouble
            val lat = fields(2).toDouble
            (name, num, lon, lat)
        }).cache()

        // 建立站点名字和编号之间的映射的广播变量
        val stationNumMap = sc.broadcast(stationInfo.map(x => (x._1, x._2)).collect().toMap)

        // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
        val odPair = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val dt = transTimeToTimestamp(fields(4))
            val halfHours = halfHourOfMonth(dt)
            val os = fields(2)
            val ds = fields(5)
            ((os, ds), halfHours)
        })

        // 按照OD分组
        val groupByOD = odPair.groupByKey().mapValues(line => {
            val total = line.size
            val groupByHalfCount = line.toList.groupBy(x => x).mapValues(_.size)
            val data = new ListBuffer[Int]
            for (i <- 0.until(1440)){
                data.append(groupByHalfCount.getOrElse(i, 0))
            }
            data.append(total)
            data.toList
        })

        val result = groupByOD.map(line => {
            val os = line._1._1
            val ds = line._1._2
            val on = stationNumMap.value.getOrElse(os, 0)
            val dn = stationNumMap.value.getOrElse(ds, 0)
            val data = line._2.mkString(",")
            os + ',' + ds + ',' + on.toString + ',' + dn.toString + ',' + data
        })

        result.repartition(1).sortBy(x => x).saveAsTextFile(args(0) + "/zlt/RCB/ODPeriodicInFlow")

        sc.stop()

    }
}
