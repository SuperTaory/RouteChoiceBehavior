import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object ODFeatureExtraction {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("OD feature extraction").getOrCreate()
    val sc = spark.sparkContext

    // 读取换乘站点名信息
    val trans_station_name = sc.textFile(args(0) + "/liutao/AllInfo/trans_station_name.txt")
    val tr_sta_name = sc.broadcast(trans_station_name.collect().toSet)

    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt")
    val stationNoToNameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

    // 读取所有有效路径的数据:1 2 # 0 V 0.0000 2.6000
    val validPathFile = sc.textFile(args(0) + "/liutao/AllInfo/allpath.txt").map(line => {
      val other = line.split(" ").takeRight(4)
      val transNum = other.head
      val costTime = other.last.toFloat.formatted("%.2f")
      // 站点编号信息
      val fields = line.split(' ').dropRight(5)
      val sou = stationNoToName.value(fields.head.toInt)
      val des = stationNoToName.value(fields.last.toInt)
      val pathStations = new ListBuffer[String]
      fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
      ((sou, des), (pathStations.toList, transNum, costTime))
    }).groupByKey()
      .mapValues(line => {
        val data = line.toList
        val newData = new ListBuffer[(List[String], String, String, Int)]
        // 给同一OD的多条有效路径编号1、2、3、、、
        for (i <- data.indices) {
          newData.append((data(i)._1, data(i)._2, data(i)._3, i+1))
        }
        newData.toList
      })
    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
    val validPathMap = sc.broadcast(validPathFile.collect().toMap)


    // 读取groundTruth:(668367478,ECD09FC6C6C5,24.0,24,1.0)
    val groundTruth = sc.textFile(args(0) + "/liutao/UI/GroundTruth/IdMap/part-00000").map(line => {
      val fields = line.split(",")
      val afcID = fields.head.drop(1)
      val apID = fields(1)
      val score = fields.last.dropRight(1).toFloat
      (afcID, apID, score)
    }).filter(_._3 > 0.7)
      .map(x => (x._1, x._2))
      .collect()
      .toMap

    val groundTruthMap = sc.broadcast(groundTruth)
    val AfcID = sc.broadcast(groundTruth.keys.toSet)
    val ApID = sc.broadcast(groundTruth.values.toSet)

    // 读取AFC数据:(669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
    val AFCData = sc.textFile(args(0) + "/liutao/UI/GroundTruth/afcData/part-*").map(line => {
      val fields = line.split(",")
      val id = fields.head.drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val os = fields(2)
      val dt = transTimeToTimestamp(fields(4))
      val ds = fields(5)
      (id, (os, ot, ds,dt))
    }).filter(x => AfcID.value.contains(x._1)).cache()

    // 挑选出客流量最大的100个OD并且OD均不是换乘站
    val selectOD = AFCData.map(x => ((x._2._1, x._2._3), 1))
      .filter(x => !tr_sta_name.value.contains(x._1._1) && !tr_sta_name.value.contains(x._1._2))
      .reduceByKey(_+_)
      .sortBy(_._2, ascending = false)

    val select_od = sc.broadcast(selectOD.map(_._1).collect().toSet)

    // 将AFC数据按照ID分组
    val PersonalAFCData = AFCData.groupByKey().mapValues(x => x.toList.sortBy(_._2))

    // 读取AP数据:(000000000000,2019-06-30 16:44:45,老街,0),并按照id分组
    val APFile = sc.textFile(args(0) + "/liutao/UI/NormalMacData/part-*").map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val stamp = transTimeToTimestamp(fields(1))
      val station = fields(2)
      val stay = fields.last.dropRight(1).toLong
      (id, (stamp, station, stay))
    }).filter(x => ApID.value.contains(x._1))
      .groupByKey()
      .mapValues(_.toList.sortBy(_._1))

    val ApData = sc.broadcast(APFile.collect().toMap)

    // 将afc数据和ap数据结合，根据前面的select OD生成每次trip对应的afc和ap记录：(String, Long, String, Long), List[(Long, String, Long)]
    val mergedData = PersonalAFCData.flatMap(line => {
      val afcId = line._1
      val apId = groundTruthMap.value(afcId)
      val apData = ApData.value(apId)
      val afcData = line._2
      val trip_afc_ap = new ListBuffer[((String, Long, String, Long), List[(Long, String, Long)])]
      for (od <- afcData) {
        if (select_od.value.contains((od._1, od._3))) {
          val l = apData.indexWhere(_._1 > od._2 - 300)
          val r = apData.lastIndexWhere(_._1 < od._4 + 300)
          val part = apData.slice(l, r+1)
          if (part.nonEmpty)
            trip_afc_ap.append((od, part))
        }
      }
      for (v <- trip_afc_ap.toList) yield v
    })

    //提取每个OD包含的每条有效路径的特征：平均花费时间、换乘次数，换乘站平均停留时间，选择的比例
    val extractFeature = mergedData.map(line => {
      val od = (line._1._1, line._1._3)
      val costTime = line._1._4 - line._1._2
      val part = line._2
      val validPaths = validPathMap.value(od)
      val temp = new ListBuffer[(String, Int)]
      var path_number = 0
      var trans_num = -1
      for (pathInfo <- validPaths) {
        var m = 0
        var n = 0
        val path = pathInfo._1
        while (m < path.length && n < part.length) {
          if (path(m) == part(n)._2) {
            m += 1
            n += 1
          }
          else
            m += 1
        }
        if (n >= part.length)
          temp.append((pathInfo._2, pathInfo._4))
      }
      if (temp.length == 1) {
        path_number = temp.head._2
        trans_num = temp.head._1.toInt
      }
      val trans_time_info = part.filter(x => tr_sta_name.value.contains(x._2))
      ((od._1, od._2, path_number), (costTime, trans_num, trans_time_info))
    }).filter(x => x._1._3 != 0)

    // 按照OD和有效路径编号分组并提取特征
    val eachValidPath = extractFeature.groupByKey().map(line => {
      val info = line._2.toList
      val trans_num = info.head._2
      val num = info.length
      val averageCostTime = info.map(_._1).sum / num
      val trans_cost_time = new ListBuffer[(String, Long)]
      for (l <- info; s <- l._3) {
        trans_cost_time.append((s._2, s._3))
      }
      val average_trans_cost_time = trans_cost_time.groupBy(_._1)
        .mapValues(v => v.toList.map(_._2).sum / v.size)
        .toList
        .sortBy(_._2)

      var average_trans_cost_time_string = ""
      var total_average_trans_cost_time = 0L
      if (trans_num != 0){
//        average_trans_cost_time.takeRight(trans_num)
//          .foreach(x => average_trans_cost_time_string += x._1 + ":" + x._2.toString + ",")
        total_average_trans_cost_time = average_trans_cost_time.map(_._2).sum
      }
      else{
//        average_trans_cost_time_string = "None"
      }

//      ((line._1._1, line._1._2), (line._1._3, averageCostTime, trans_num, average_trans_cost_time_string.dropRight(1), num))
      ((line._1._1, line._1._2), (line._1._3, averageCostTime, trans_num, total_average_trans_cost_time, num))
    }).cache()

    val ODNum = eachValidPath.groupByKey()
      .filter(_._2.size > 1)
      .repartition(1)
      .mapValues(_.toList.map(_._5).sum)
      .sortBy(_._2, ascending = false)
      .cache()

    val k_th_num = ODNum.take(100).last._2

//    ODNum.saveAsTextFile(args(0) + "/liutao/RCB/ODNum")

    val result = eachValidPath.groupByKey().filter(_._2.size > 1).flatMap(line => {
      val data = line._2.toList
      val sum_of_num = data.map(_._5).sum
      for (v <- data) yield {
        (line._1._1, line._1._2, v._1, v._2, v._3, v._4, v._5, sum_of_num, (v._5 / sum_of_num.toFloat).formatted("%.2f"))
      }
    }).filter(_._8 >= k_th_num)

    result.repartition(1)
      .sortBy(x => (x._1, x._2, x._3))
      .saveAsTextFile(args(0) + "/liutao/RCB/ODFeatureExtraction")


    sc.stop()
  }
}
