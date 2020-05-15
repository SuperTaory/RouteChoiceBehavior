import org.apache.spark.sql.SparkSession
import CommonFunctions.transTimeToTimestamp

import scala.collection.mutable.ListBuffer

object PersonalRouteChoice {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Personal Route Choice").getOrCreate()
    val sc = spark.sparkContext

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
          newData.append((data(i)._1,data(i)._2, data(i)._3, i+1))
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
    }).filter(x => AfcID.value.contains(x._1))
      .groupByKey()
      .mapValues(v => {
        val data = v.toList.sortBy(_._2)
        val pairs = data.map(x => (x._1, x._3))
        val countPair = pairs.groupBy(x => x).mapValues(_.length).filter(_._2 > 3).keys
        (data, countPair.toSet)
      })


    // 读取AP数据:(000000000000,2019-06-30 16:44:45,老街,0)
    val APFile = sc.textFile(args(0) + "/liutao/UI/NormalMacData/part-*").map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val stamp = transTimeToTimestamp(fields(1))
      val station = fields(2)
      (id, (stamp, station))
    }).filter(x => ApID.value.contains(x._1))
      .groupByKey()
      .mapValues(_.toList.sortBy(_._1))

    val ApData = sc.broadcast(APFile.collect().toMap)


    // 将AFC和AP数据按照匹配关系结合并处理
    val mergedData = AFCData.flatMap(line => {
      val afcId = line._1
      val apId = groundTruthMap.value(line._1)
      val apData = ApData.value(apId)
      val pairs = line._2._2
      val afcData = line._2._1
      val records = new ListBuffer[(String, String, (String, String, Int))]
      for (p <- afcData) {
        if (pairs.contains((p._1, p._3))) {
          val validPaths = validPathMap.value((p._1, p._3))
          val l = apData.indexWhere(_._1 > p._2 - 300)
          val r = apData.lastIndexWhere(_._1 < p._4 + 300)
          val part = apData.slice(l, r+1)
          val temp = new ListBuffer[(String, String, Int)]
          if (part.nonEmpty) {
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
                temp.append((pathInfo._2, pathInfo._3, pathInfo._4))
            }
            if (temp.length == 1)
              records.append((p._1, p._3, temp.head))
          }
        }
      }
      val result = new ListBuffer[(String, String, Float, String, Int, String, String)]
      val ODCountMap = records.toList.groupBy(x => (x._1, x._2)).mapValues(_.size)
      val countRatio = records.toList.groupBy(x => x).map(line => {
        val pair = (line._1._1, line._1._2)
        val ratio = line._2.length.toFloat / ODCountMap(pair)
        val ratioStr = line._2.length.toString + "/" + ODCountMap(pair).toString
        (line._1, ratio, ratioStr, line._1._3._3)
      })
      countRatio.foreach(x => result.append((x._1._1, x._1._2, x._2, x._3, x._4, x._1._3._1, x._1._3._2)))

      for (v <- result.toList) yield {
        (afcId, v._1, v._2, v._3, v._4, v._5, v._6, v._7)
      }
    })

    mergedData.repartition(1)
      .sortBy(x => (x._1, x._2, x._3))
      .saveAsTextFile(args(0) + "/liutao/RCB/PersonalRouteChoice")

    sc.stop()
  }
}
