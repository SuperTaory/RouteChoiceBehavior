import CommonFunctions._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._

object NewFeatures {
    def main(args: Array[String]): Unit = {
        /**
         * 统计群体流量分布特征，全天和分时
         */
        val spark = SparkSession.builder().appName("NewFeatures").getOrCreate()
        val sc = spark.sparkContext

        case class distAndKinds(var d:Long, var k:Int)

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
            val ot = hourOfDay(fields(1)) / 2
            val dt = hourOfDay(fields(4)) / 2
            val os = stationNumMap.value(fields(2))
            val ds = stationNumMap.value(fields(5))
            val day = dayOfMonth_string(fields(1))
            ((day, ot), (os, ds))
        })

        // *********************  群体  *********************

        // 各站点间分时段平均流量情况
        val periodFlow = odPair.groupByKey()
            .mapValues(data=>{
                val count = data.groupBy(x => x).mapValues(_.size)
                val matrix = Array.ofDim[Float](166,166)
                for ( p <- count.keys) {
                    matrix(p._1)(p._2) = count(p)
                }

                for (i <- 0.until(166)){
                    val s = matrix(i).sum
                    for (j <- 0.until(166)){
                        matrix(i)(j) = matrix(i)(j) / s
                    }
                }
                matrix
            })
        val periodGroupFlow = sc.broadcast(periodFlow.collect().toMap)


        // *********************  个体  *********************

        // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
        val odPairWithID = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val dt = transTimeToTimestamp(fields(4))
            val os = stationNumMap.value(fields(2))
            val ds = stationNumMap.value(fields(5))
            val o_day = dayOfMonth_string(fields(1))
            val d_day = dayOfMonth_string(fields(4))
            val day = if (o_day == d_day) o_day else 0
            (id, (ot, os, dt, ds, day))
        }).filter(_._2._5 > 0)

        // 按照ID分组
        val groupByID = odPairWithID
            .groupByKey()
            .map(line => {
                val dataArray = line._2.toList.sortBy(_._1)
                val daySets = dataArray.map(_._5).toSet
                (line._1, dataArray, daySets)
            }).filter(x => x._3.size > 5)

        // 筛选不规律出行
        val irregularTrip = groupByID.map(line => {

            val pairs = line._2
            val daySets = line._3
            // 聚类
            val stampBuffer = new ArrayBuffer[Long]()
            pairs.foreach(v => {
                stampBuffer.append(secondsOfDay(v._1))
                stampBuffer.append(secondsOfDay(v._3))
            })
            val timestamps = stampBuffer.toArray.sorted

            // 设置带宽h，单位为秒
            val h = 1800
            // 计算局部密度
            val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
            for (t <- timestamps) {
                var temp = 0D
                for (v <- timestamps) {
                    temp += RBF(v, t, h)
                }
                density_stamp_Buffer.append((temp / (timestamps.length * h), t))
            }
            val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

            // 判断是否存在聚类中心，若返回为空则不存在，否则分类
            val cluster_center = z_score(density_stamp)

            var tripIndex = -1
            var havePattern = 0
            // 可能存在出行模式时
            if (cluster_center.nonEmpty){
                // 设置类边界距离并按照聚类中心分配数据
                val dc = 5400
                // 初始化类簇,结构为[所属类，出行片段]
                val clusters = new ArrayBuffer[(Int, (Long, Int, Long, Int, Int))]
                for (v <- pairs) {
                    val o_stamp = secondsOfDay(v._1)
                    val d_stamp = secondsOfDay(v._3)
                    val o_to_c = distAndKinds(Long.MaxValue, 0)
                    val d_to_c = distAndKinds(Long.MaxValue, 0)
                    for (c <- cluster_center) {
                        if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d){
                            o_to_c.k = c._1
                            o_to_c.d = abs(o_stamp - c._2)
                        }
                        if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d){
                            d_to_c.k = c._1
                            d_to_c.d = abs(d_stamp - c._2)
                        }
                    }
                    if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
                        clusters.append(( o_to_c.k, v))
                    else
                        clusters.append((0, v))
                }

                // 存储出行模式集合
                val afc_patterns = new ArrayBuffer[(Int, Int)]()

                val groupByPair = pairs.groupBy(x => (x._2, x._4)).mapValues(_.size)
                for (k <- groupByPair.keys) {
                    if (groupByPair(k) + groupByPair.getOrElse((k._2, k._1), 0) > pairs.length / 2) {
                        afc_patterns.append(k)
                    }
                }

                // 按照所属类别分组
                val grouped = clusters.toArray.filter(x => x._1 > 0).groupBy(_._1)
                if (grouped.nonEmpty){
                    grouped.foreach(g => {
                        // 同一类中数据按照进出站分组
                        val temp_data = g._2.groupBy(x => (x._2._2, x._2._4))
                        temp_data.foreach(v => {
                            // 超过总出行天数的1/2则视为出行模式
                            if ( v._2.length >= 5 || v._2.length > daySets.size / 2) {
                                afc_patterns.append(v._1)
                            }
                        })
                    })
                }

                if (afc_patterns.isEmpty) {
                    tripIndex = pairs.length - 1
                }
                else {
                    val len = pairs.length - 1
                    var flag = true
                    for (i <- len.to(0, -1) if flag) {
                        val tr = (pairs(i)._2, pairs(i)._4)
                        if (!afc_patterns.contains(tr)) {
                            tripIndex = i
                            havePattern = 1
                            flag = false
                        }
                    }
                }
            }
            else{
                tripIndex = pairs.length - 1
            }

            // 群体时空特征
            val crowdFeatures = new ArrayBuffer[Array[Float]]()
            // 个体特征
            val featureTD = Array.ofDim[Int](2,166)
            val featureSD = Array.ofDim[Int](166)
            val featureTSD = Array.ofDim[Int](2, 166)
            val featureTSI = Array.ofDim[Int](166)
            if (tripIndex > 0) {
                // 提取历史出行数据
                val hisTrips = pairs.take(tripIndex)
                val weekday = hisTrips.filter(x => dayOfWeek(x._1) > 1 & dayOfWeek(x._1) < 7)
                val weekend = hisTrips.filter(x => dayOfWeek(x._1) == 1 | dayOfWeek(x._1) == 7)
                val trip = pairs(tripIndex)

                // featureTD
                val weekdayFlow = weekday.map(x => (periodOfDay(x._1), x._4)).groupBy(x => x).mapValues(_.size)
                    .map(x => (x._1._1, (x._1._2, x._2)))
                    .groupBy(x => x._1)
                    .mapValues(v => v.values.toList)
                if (weekdayFlow.contains(periodOfDay(trip._1))) {
                    for (v <- weekdayFlow(periodOfDay(trip._1)))
                        featureTD(0)(v._1) = v._2
                }

                val weekendFlow = weekend.map(x => (periodOfDay(x._1), x._4)).groupBy(x => x).mapValues(_.size)
                    .map(x => (x._1._1, (x._1._2, x._2)))
                    .groupBy(x => x._1)
                    .mapValues(v => v.values.toList)
                if (weekendFlow.contains(periodOfDay(trip._1))) {
                    for (v <- weekendFlow(periodOfDay(trip._1)))
                        featureTD(1)(v._1) = v._2
                }
                // featureSD
                val fixOs = hisTrips.map(x => (x._2, x._4)).groupBy(x => x).mapValues(_.size)
                    .map(x => (x._1._1, (x._1._2, x._2)))
                    .groupBy(x => x._1)
                    .mapValues(v => v.values.toList)
                if (fixOs.contains(trip._2)) {
                    for (v <- fixOs(trip._2))
                        featureSD(v._1) = v._2
                }

                // featureTSD
                val weekdayTSD = weekday.map(x => ((periodOfDay(x._1), x._2), x._4)).groupBy(_._1).mapValues(v => {
                    v.map(_._2).groupBy(x=>x).mapValues(_.size).toList
                })
                if (weekdayTSD.contains((periodOfDay(trip._1), trip._2))) {
                    for (v <- weekdayTSD((periodOfDay(trip._1), trip._2)))
                        featureTSD(0)(v._1) = v._2
                }
                val weekendTSD = weekend.map(x => ((periodOfDay(x._1), x._2), x._4)).groupBy(_._1).mapValues(v => {
                    v.map(_._2).groupBy(x=>x).mapValues(_.size).toList
                })
                if (weekendTSD.contains((periodOfDay(trip._1), trip._2))) {
                    for (v <- weekendTSD((periodOfDay(trip._1), trip._2)))
                        featureTSD(1)(v._1) = v._2
                }

                // featureTSI
                hisTrips.foreach(x => {
                    featureTSI(x._2) += 1
                    featureTSI(x._4) += 1
                })

                // 提取群体流量分布特征
                var day = trip._5 - 1
                val slot = hourOfDay_Long(trip._1) / 2
                val os = trip._2
                var M = 1
                val emptyFlow = Array.ofDim[Float](166)
                while (M > 0 & day > 0) {
                    var week = Array.ofDim[Float](166)
                    for (i <- 1.to(7) if day > 0) {
                        if (periodGroupFlow.value.contains((day, slot)))
                            week = week.zip(periodGroupFlow.value(day, slot)(os)).map(x => x._1 + x._2)
                        else
                            week = week.zip(emptyFlow).map(x => x._1 + x._2)
                        day -= 1
                    }
                    crowdFeatures.append(week)
                    M -= 1
                }
                if (crowdFeatures.isEmpty)
                    crowdFeatures.append(emptyFlow)
                // 目的站点
                val target = trip._4
                // 起始时间特征[0-11]
                val st = hourOfDay_Long(trip._1) / 2

                val crowdFeatureString = crowdFeatures.toArray.head.map(_.formatted("%.3f")).mkString(",")
                val TDString = featureTD(0).mkString(",") + "#" + featureTD(1).mkString(",")
                val SDString = featureSD.mkString(",")
                val TSDString = featureTSD(0).mkString(",") + "#" + featureTSD(1).mkString(",")
                val TSIString = featureTSI.mkString(",")

                trip._2.toString + ":" + crowdFeatureString + "#" + TDString + "#" + SDString + "#" + TSDString + "#" +
                    TSIString + ":" + target.toString + ":" + st.toString + ":" + tripIndex.toString
            }
            else
                ""
        }).filter(x => x.nonEmpty)

        irregularTrip.repartition(5).saveAsTextFile(args(0) + "zlt/RCB-2021/IrTripFeatures")

        sc.stop()
    }


    def RBF(l : Long, x : Long, h: Int) : Double = {
        1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
    }


    def z_score(dens_pos : Array[(Double, Long)]) : Array[(Int, Long)] = {
        val dist_r = compute_dist(dens_pos)
        val dist_l = compute_dist(dens_pos.reverse).reverse
        val dist_dens_pos = new ArrayBuffer[(Long, Double, Long)]()
        for (i <- dist_r.indices) {
            if (dist_r(i) == -1 && dist_l(i) == -1)
                dist_dens_pos.append((dens_pos.last._2 - dens_pos.head._2, dens_pos(i)._1, dens_pos(i)._2))
            else if (dist_r(i) != -1 && dist_l(i) != -1)
                dist_dens_pos.append((min(dist_r(i), dist_l(i)), dens_pos(i)._1, dens_pos(i)._2))
            else if (dist_l(i) != -1)
                dist_dens_pos.append((dist_l(i), dens_pos(i)._1, dens_pos(i)._2))
            else
                dist_dens_pos.append((dist_r(i), dens_pos(i)._1, dens_pos(i)._2))
        }
        var sum_dist = 0L
        var sum_dens = 0d
        dist_dens_pos.foreach(x => {
            sum_dist += x._1
            sum_dens += x._2
        })
        val avg_dist = sum_dist / dist_dens_pos.length
        val avg_dens = sum_dens / dist_dens_pos.length
        var total = 0d
        for (v <- dist_dens_pos) {
            total += pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)
        }
        val sd = sqrt(total / dist_dens_pos.length)
        val z_score = new ArrayBuffer[((Long, Double, Long), Double)]()
        var z_value = 0d
        for (v <- dist_dens_pos) {
            z_value = sqrt(pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)) / sd
            z_score.append((v, z_value))
        }
        val result = new ArrayBuffer[(Int, Long)]()
        // z-score大于3认为是类簇中心
        val clustersInfo = z_score.toArray.filter(_._2 >= 3)
        for (i <- clustersInfo.indices) {
            result.append((i+1, clustersInfo(i)._1._3))
        }
        result.toArray
    }

    // 计算相对距离
    def compute_dist(info : Array[(Double, Long)]) : Array[Long] = {
        val result = new Array[Long](info.length)
        val s = mutable.Stack[Int]()
        s.push(0)
        var i = 1
        var index = 0
        while (i < info.length) {
            if (s.nonEmpty && info(i)._1 > info(s.top)._1) {
                index = s.pop()
                result(index) = abs(info(i)._2 - info(index)._2)
            }
            else{
                s.push(i)
                i += 1
            }
        }
        while (s.nonEmpty) {
            result(s.pop()) = -1
        }
        result
    }
}
