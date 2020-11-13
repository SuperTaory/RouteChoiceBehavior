import org.apache.spark.sql.SparkSession
import CommonFunctions.{halfHourOfMonth, hourOfDay, transTimeToTimestamp}

import scala.collection.mutable.ListBuffer

object ODPeriodicFlow {
    def main(args: Array[String]): Unit = {
        /**
         * 统计每个OD pair之间的周期流量
         */
        val spark = SparkSession.builder().appName("ODPeriodicFlow").getOrCreate()
        val sc = spark.sparkContext

        // 读取站点信息 : 1,机场东,22.647011,113.8226476,1268036000,268
        // 站点编号0-165
        val stationInfo = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt").map(line => {
            val fields = line.split(",")
            val num = fields.head.toInt - 1
            val name = fields(1)
            (name, num)
        })

        // 建立站点名字和编号之间的映射的广播变量
        val stationNumMap = sc.broadcast(stationInfo.collect().toMap)

        // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
        val odPair = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val ot = hourOfDay(fields(1))
            val dt = hourOfDay(fields(4))
            val os = stationNumMap.value(fields(2))
            val ds = stationNumMap.value(fields(5))
            ((os, ds), ot, dt)
        }).cache()

        val leavingPattern = odPair.map(x => (x._1, x._2))
        val arrivingPattern = odPair.map(x => (x._1, x._3))

        // leaving pattern count
        val leavingMatrix = leavingPattern.groupByKey().mapValues(line => {
            val groupCount = line.toList.groupBy(x => x).mapValues(_.size)
            val data = new ListBuffer[Int]
            for (i <- 0.until(24)){
                data.append(groupCount.getOrElse(i, 0))
            }
            data.toList.mkString(",")
        })

        val groupByOri = leavingMatrix.map(line => (line._1._1, (line._1._2, line._2))).groupByKey()

//        leavingMatrix.repartition(1).sortBy(x => x).saveAsTextFile(args(0) + "/zlt/RCB/MobilityPatternCount/leaving")

        // arriving pattern count
        val arrivingMatrix = arrivingPattern.groupByKey().mapValues(line => {
            val groupCount = line.toList.groupBy(x => x).mapValues(_.size)
            val data = new ListBuffer[Int]
            for (i <- 0.until(24)){
                data.append(groupCount.getOrElse(i, 0))
            }
            data.toList.mkString(",")
        })
        //        arrivingMatrix.repartition(1).sortBy(x => x).saveAsTextFile(args(0) + "/zlt/RCB/MobilityPatternCount/arriving")

        val groupByDes = arrivingMatrix.map(line => (line._1._2, (line._1._1, line._2))).groupByKey()

        val joinRDD = groupByOri.join(groupByDes).map(line => {
            val defaultString = Array.ofDim[Int](24).mkString(",")
            val desString = new ListBuffer[String]
            val desMap = line._2._1.toMap
            for (i <- 0.to(165)){
                desString.append(desMap.getOrElse(i, defaultString))
            }

            val oriString = new ListBuffer[String]
            val oriMap = line._2._2.toMap
            for (i <- 0.to(165)){
                oriString.append(oriMap.getOrElse(i, defaultString))
            }
            (line._1 , desString.mkString(",") + ',' + oriString.mkString(","))
        })

        joinRDD.repartition(1).sortByKey(ascending = true).map(line => line._1.toString + "," + line._2)
            .saveAsTextFile(args(0) + "/zlt/RCB/MobilityPatternCount/freqMatrix")
        sc.stop()

    }
}
