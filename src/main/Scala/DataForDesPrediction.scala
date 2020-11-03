import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import CommonFunctions.{transTimeToTimestamp, dayOfWeek, quarterOfDay, hourOfDay}

import scala.collection.mutable.ListBuffer


object DataForDesPrediction {


    case class GPS(lon: Double, lat: Double)

    def main(args: Array[String]): Unit = {
        /**
         * 筛选出一个站点，从该站点进站的乘客数量多且出站站点分布广
         */
        val spark = SparkSession.builder()
            .appName("DataForDesPrediction")
            .getOrCreate()
        val sc = spark.sparkContext

        import spark.implicits._

        // 读取站点信息 : 1,机场东,22.647011,113.8226476,1268036000,268
        val stationInfo = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt").map(line => {
            val fields = line.split(",")
            val num = fields.head.toInt - 1
            val name = fields(1)
            val lon = fields(3).toDouble
            val lat = fields(2).toDouble
            (name, num, lon, lat)
        }).cache()

        val stationNumMap = sc.broadcast(stationInfo.map(x => (x._1, x._2)).collect().toMap)

        // 找出所有站点GPS经纬度的最大值最小值
        val gps = stationInfo.map(x => (x._3, x._4)).collect()
        val lon_max = gps.maxBy(_._1)._1
        val lon_min = gps.minBy(_._1)._1
        val lat_max = gps.maxBy(_._2)._2
        val lat_min = gps.minBy(_._2)._2

        def normalizedLon(x : Double) : Double = {
            (x - lon_min) / (lon_max - lon_min)
        }

        def normalizedLat(x : Double) : Double = {
            (x - lat_min) / (lat_max - lat_min)
        }

        // 将GPS标准化: (x - x.min) / (x.max -x.min)
//        val stationGPSMap = sc.broadcast(stationInfo.map(x => (x._2, (normalizedLon(x._3), normalizedLat(x._4)))).collect().toMap)
        val stationGPSMap = sc.broadcast(stationInfo.map(x => (x._2, (x._3, x._4))).collect().toMap)

        // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
        val inOut = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val in = fields(2)
            val out = fields(5)
            (in, out)
        })

        //  统计每个站点作为进站时的出行次数以及出站站点的集合大小
        val stat = inOut.groupByKey().mapValues(v => {
            val num = v.size
            val outStationSet = v.toSet
            (num, outStationSet.size)
        })

        val res = stat.repartition(1).sortBy(_._2, ascending = false)

        //    res.saveAsTextFile(args(0) + "/liutao/RCB/InOutStat")

        // 找出作为进站站点的出行次数最多的站点编号A
        val A = stationNumMap.value(res.first()._1)

        /**
         * 根据上述统计确定进站站点A并提取以A作为进站的出行的数据，作为训练数据
         */
        // 读取AFC数据 (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
        val morningPeak = Set(7,8,9)
        val eveningPeak = Set(17,18,19)
        val tripData = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = stationNumMap.value(fields(2))
            val o_gps = stationGPSMap.value(os)
            val ds = stationNumMap.value(fields(5))
            val d_gps = stationGPSMap.value(ds)
            val day = if (dayOfWeek(ot) == 1 || dayOfWeek(ot) == 7) 1 else 0
            val hour = hourOfDay(fields(1))
            var mp = 0
            var ep = 0
            var normal = 0
            if (morningPeak.contains(hour))
                mp = 1
            else if (eveningPeak.contains(hour))
                ep = 1
            else
                normal = 1

            (id, (ot, os, ds, o_gps, d_gps, mp, ep, normal, day))
        })

        // 切片，取进站站点为A的出行及其前k次出行的记录
        val cutToSamples = tripData.groupByKey().mapValues(_.toList.sortBy(_._1)).flatMap(line => {
            val trips = line._2
            val windowSize = args(1).toInt   // k
            var i = 0
            val prefix = new ListBuffer[List[(Double, Double, Double, Double, Int, Int, Int, Int)]]
            val temp = new ListBuffer[(Double, Double, Double, Double, Int, Int, Int, Int)]
            val lastTrip = new ListBuffer[(Double, Double, Int, Int, Double, Double, Int)]
            var prefix_len = 0
            while (i < trips.length) {
                if (trips(i)._2 == A) {
                    if (i < windowSize) {
//                        prefix_len = i
//                        for (j <- 0.until(i)) {
//                            temp.append((trips(j)._4._1, trips(j)._4._2, dayOfWeek(trips(j)._1), quarterOfDay(trips(j)._1), trips(j)._5._1, trips(j)._5._2))
//                        }
//                        for (j <- 0.until(windowSize - i)) {
//                            temp.append((0, 0, 0, 0, 0, 0))
//                        }
                    }
                    else {
                        prefix_len = windowSize
                        for (j <- (i - windowSize).until(i)) {
                            val t = trips(j)
                            temp.append((t._4._1, t._4._2, t._5._1, t._5._2, t._6, t._7, t._8, t._9))
                        }
                    }
                    val last_trip = trips(i)
                    val day = dayOfWeek(last_trip._1)
                    val quarter = quarterOfDay(last_trip._1)
                    lastTrip.append((last_trip._4._1, last_trip._4._2, day, quarter, last_trip._5._1, last_trip._5._2, prefix_len))
                    prefix.append(temp.toList)
                    temp.clear()
                }
                i += 1
            }

            for (i <- prefix.indices) yield {
                (prefix(i), lastTrip(i))
            }
        }).filter(_._1.nonEmpty)

        val samples = cutToSamples.map(line => {
            val pre = line._1
            val lastTrip = line._2
            pre.map(x => x.toString().drop(1).dropRight(1)).mkString(",") + "," + lastTrip.toString().drop(1).dropRight(1)
        })
        samples.coalesce(1).saveAsTextFile(args(0) + "/zlt/RCB/SamplesForDesPre/Unnormalized_" + args(1))

//      samples.coalesce(1).write.option("header", "true").csv(args(0) + "/zlt/RCB/SamplesForDesPre/prefix_len_" + args(1))

        sc.stop()
    }


}
