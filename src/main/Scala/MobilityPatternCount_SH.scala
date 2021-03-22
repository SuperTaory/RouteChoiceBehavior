import org.apache.spark.sql.SparkSession
import CommonFunctions.{halfHourOfMonth, hourOfDay, transTimeToTimestamp}

import scala.collection.mutable.ListBuffer

object MobilityPatternCount_SH {
    def main(args: Array[String]): Unit = {
        /**
         * 统计每个OD pair之间的周期流量
         */
        val spark = SparkSession.builder().appName("MobilityPatternCount_SH").getOrCreate()
        val sc = spark.sparkContext

        // 读取站点信息 : (270,8号线成山路) 站点编号0-312
        val stationInfo = sc.textFile(args(0) + "/SH/SubwayInfo/*").map(line => {
            val fields = line.drop(1).dropRight(1).split(",")
            val num = fields.head.toInt
            val name = fields(1)
            (name, num)
        })
        val stationNumMap = sc.broadcast(stationInfo.collect().toMap)

        // 读取上海AFC数据 (2100276033,2015-04-03 15:39:56,7号线昌平路,0.00,2015-04-03 15:40:47,7号线昌平路,3.00)
        val odPair = sc.textFile(args(0) + "/SH/SubwayPair/*").map(line => {
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
            for (i <- 0.to(312)){
                desString.append(desMap.getOrElse(i, defaultString))
            }

            val oriString = new ListBuffer[String]
            val oriMap = line._2._2.toMap
            for (i <- 0.to(312)){
                oriString.append(oriMap.getOrElse(i, defaultString))
            }
            (line._1 , desString.mkString(",") + ',' + oriString.mkString(","))
        })

        joinRDD.repartition(1).sortByKey(ascending = true).map(line => line._1.toString + "," + line._2)
            .saveAsTextFile(args(0) + "/zlt/RCB-2021/MobilityPatternCount_SH")
        sc.stop()

    }
}
