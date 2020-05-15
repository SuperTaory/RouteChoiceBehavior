import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import CommonFunctions.{transTimeToTimestamp, dayOfWeek, quarterOfDay}

import scala.collection.mutable.ListBuffer


object DataForDesPrediction {

    //  case class Samples(o1:Int, d1:Int, o2:Int, d2:Int, o3:Int, d3:Int, o4:Int, d4:Int, o5:Int, d5:Int,
    //                     o6:Int, d6:Int, o7:Int, d7:Int, o8:Int, d8:Int, o9:Int, d9:Int, ox:Int, dx:Int,
    //                     lo:Int, day:Int, quarter:Int, ld:Int)
    case class Samples(o_lon1: Double, o_lat1: Double, ot1: Int, d_lon1: Double, d_lat1: Double, o_lon2: Double, o_lat2: Double, ot2: Int, d_lon2: Double, d_lat2: Double,
                       o_lon3: Double, o_lat3: Double, ot3: Int, d_lon3: Double, d_lat3: Double, o_lon4: Double, o_lat4: Double, ot4: Int, d_lon4: Double, d_lat4: Double,
                       o_lon5: Double, o_lat5: Double, ot5: Int, d_lon5: Double, d_lat5: Double, o_lon6: Double, o_lat6: Double, ot6: Int, d_lon6: Double, d_lat6: Double,
                       o_lon7: Double, o_lat7: Double, ot7: Int, d_lon7: Double, d_lat7: Double, o_lon8: Double, o_lat8: Double, ot8: Int, d_lon8: Double, d_lat8: Double,
                       o_lon9: Double, o_lat9: Double, ot9: Int, d_lon9: Double, d_lat9: Double, o_lonx: Double, o_latx: Double, otx: Int, d_lonx: Double, d_latx: Double,
                       lo_lon: Double, lo_lat: Double, day: Int, quarter: Int, ld_lon: Double, ld_lat: Double)

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
        val stationInfo = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt").map(line => {
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

        // 将GPS标准化
        val stationGPSMap = sc.broadcast(stationInfo.map(x => (x._2, (normalizedLon(x._3), normalizedLat(x._4)))).collect().toMap)

        // 读取AFC数据
        val inOut = sc.textFile(args(0) + "/Destin/subway-pair/*").map(line => {
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
        val tripData = sc.textFile(args(0) + "/Destin/subway-pair/*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = stationNumMap.value(fields(2))
            val o_gps = stationGPSMap.value(os)
            val ds = stationNumMap.value(fields(5))
            val d_gps = stationGPSMap.value(ds)
            (id, (ot, os, ds, o_gps, d_gps))
        })

        // 切片，取进站站点为A的出行及其前k次出行的记录
        val cutToSamples = tripData.groupByKey().mapValues(_.toList.sortBy(_._1)).flatMap(line => {
            val trips = line._2
            val windowSize = 10   // k
            var i = 0
            val prefix = new ListBuffer[List[(Double, Double, Int, Double, Double)]]
            val temp = new ListBuffer[(Double, Double, Int, Double, Double)]
            val lastTrip = new ListBuffer[(Double, Double, Int, Int, Double, Double)]
            while (i < trips.length) {
                if (trips(i)._2 == A) {
                    if (i < windowSize) {
                        for (j <- 0.until(windowSize - i)) {
                            temp.append((0, 0, 0, 0, 0))
                        }
                        for (j <- 0.until(i)) {
                            temp.append((trips(j)._4._1, trips(j)._4._2, quarterOfDay(trips(j)._1), trips(j)._5._1, trips(j)._5._2))
                        }
                    }
                    else {
                        for (j <- (i - windowSize).until(i)) {
                            temp.append((trips(j)._4._1, trips(j)._4._2, quarterOfDay(trips(j)._1), trips(j)._5._1, trips(j)._5._2))
                        }
                    }
                    val last_trip = trips(i)
                    val day = dayOfWeek(last_trip._1)
                    val quarter = quarterOfDay(last_trip._1)
                    lastTrip.append((last_trip._4._1, last_trip._4._2, day, quarter, last_trip._5._1, last_trip._5._2))
                    prefix.append(temp.toList)
                    temp.clear()
                }
                i += 1
            }

            for (i <- prefix.indices) yield {
                (prefix(i), lastTrip(i))
            }
        })

        //    // 统计GPS最大最小值
        //    val minMax = cutToSamples.flatMap(line => {
        //      val prefix = line._1
        //      val last = line._2
        //      val gpsList = new ListBuffer[(Double, Double)]
        //      prefix.foreach(x => {
        //        gpsList.append((x._1.toDouble, x._2.toDouble))
        //        gpsList.append((x._4.toDouble, x._5.toDouble))
        //      })
        //      gpsList.append((last._1.toDouble, last._2.toDouble))
        //      gpsList.append((last._5.toDouble, last._6.toDouble))
        //      for (gps <- gpsList) yield
        //        gps
        //    }).map(x => GPS(x._1, x._2)).toDF("lon", "lat")
        //
        //    val lon_max = minMax.describe("lon").filter("summary='max'").select("lon").collect().map(_ (0)).toList.head.toString.toDouble
        //    val lon_min = minMax.describe("lon").filter("summary='min'").select("lon").collect().map(_ (0)).toList.head.toString.toDouble
        //
        //    val lat_max = minMax.describe("lat").filter("summary='max'").select("lat").collect().map(_ (0)).toList.head.toString.toDouble
        //    val lat_min = minMax.describe("lat").filter("summary='min'").select("lat").collect().map(_ (0)).toList.head.toString.toDouble

        val samples = cutToSamples.map(line => {
            val pre = line._1
            val lastTrip = line._2
            Samples(pre.head._1, pre.head._2, pre.head._3, pre.head._4, pre.head._5,
                pre(1)._1, pre(1)._2, pre(1)._3, pre(1)._4, pre(1)._5,
                pre(2)._1, pre(2)._2, pre(2)._3, pre(2)._4, pre(2)._5,
                pre(3)._1, pre(3)._2, pre(3)._3, pre(3)._4, pre(3)._5,
                pre(4)._1, pre(4)._2, pre(4)._3, pre(4)._4, pre(4)._5,
                pre(5)._1, pre(5)._2, pre(5)._3, pre(5)._4, pre(5)._5,
                pre(6)._1, pre(6)._2, pre(6)._3, pre(6)._4, pre(6)._5,
                pre(7)._1, pre(7)._2, pre(7)._3, pre(7)._4, pre(7)._5,
                pre(8)._1, pre(8)._2, pre(8)._3, pre(8)._4, pre(8)._5,
                pre(9)._1, pre(9)._2, pre(9)._3, pre(9)._4, pre(9)._5,
                lastTrip._1, lastTrip._2, lastTrip._3, lastTrip._4, lastTrip._5, lastTrip._6)
        }).toDF()

//        samples.printSchema()

//        samples.coalesce(10).write.option("header", "true").csv(args(0) + "/liutao/RCB/SamplesForDP/GPS_Normalized")

        println(lon_max)
        println(lon_min)
        println(lat_max)
        println(lat_min)
        sc.stop()
    }


}
