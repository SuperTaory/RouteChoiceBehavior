import org.apache.spark.sql.SparkSession
import CommonFunctions.{halfHourOfMonth, transTimeToTimestamp}

object ODAverageTime_SH {
    def main(args: Array[String]): Unit = {
        /**
         * 统计每个OD对之间的平均行程时间
         */

        val spark = SparkSession.builder().appName("ODAverageTime_SH").getOrCreate()
        val sc = spark.sparkContext

        // 读取地铁站点名和编号映射关系 (270,8号线成山路)
        val stationFile = sc.textFile(args(0) + "/SH/SubwayInfo/*")
        val stationName2NoRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0).drop(1)
            val stationName = line.split(',')(1).dropRight(1)
            (stationName, stationNo.toInt)
        })
        val stationName2No = sc.broadcast(stationName2NoRDD.collect().toMap)

        // (2100276033,2015-04-03 15:39:56,7号线昌平路,0.00,2015-04-03 15:40:47,7号线昌平路,3.00)
        val odPair = sc.textFile(args(0) + "/SH/SubwayPair/part*").map(line => {
            val fields = line.split(',')
            val ot = transTimeToTimestamp(fields(1))
            val dt = transTimeToTimestamp(fields(4))
            val os = stationName2No.value(fields(2))
            val ds = stationName2No.value(fields(5))
            ((os, ds), dt - ot)
        })

        val groupByOD = odPair.groupByKey().mapValues(_.toList).map(line=>{
            val os = line._1._1
            val ds = line._1._2
            val time = line._2.sum / line._2.length
            (os, ds, time)
        })

        groupByOD.repartition(1).saveAsTextFile(args(0) + "zlt/RCB-2021/ODAverageTime_SH")

        sc.stop()
    }

}
