import org.apache.spark.sql.SparkSession
import GeneralFunctionSets.{dayOfMonth_long, transTimeToTimestamp}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, min}

object RouteChoiceRatioFromAP {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Route Choice Ratio From AP").getOrCreate()
    val sc = spark.sparkContext

    // 读取站点间隔时间
    val readODTimeInterval = sc.textFile(args(0) + "/liutao/AllInfo/od_interval").map(line => {
      val p = line.split(',')
      val sou = p(0).drop(1)
      val des = p(1)
      val interval = p(2).dropRight(1).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt")
    val stationNoToNameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

    // 读取所有有效路径的数据
    val validPathFile = sc.textFile(args(0) + "/liutao/AllInfo/allpath.txt").map(line => {
      val other = line.split(" ").takeRight(4)
      val transNum = other.head
      val costTime = other.last.toFloat.formatted("%.2f")
      // 站点编号信息
      val fields = line.split(' ').dropRight(5)
      val sou = stationNoToName.value(fields(0).toInt)
      val des = stationNoToName.value(fields(fields.length-1).toInt)
      val pathStations = new ListBuffer[String]
      fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
      ((sou, des), (pathStations.toList, transNum, costTime))
    }).groupByKey().mapValues(line => {
      val data = line.toList
      var minLens = Int.MaxValue
      data.foreach(x => minLens = min(minLens, x._1.length))
      (data, minLens)
    })

    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
    val validPathMap = sc.broadcast(validPathFile.collect().toMap)

    // 读取AP数据 (000000000000,2019-06-30 16:44:45,老街,0)
    val APFile = sc.textFile(args(0) + "/liutao/UI/NormalMacData/part-*").map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val stamp = transTimeToTimestamp(fields(1))
      val station = fields(2)
      val stay = fields(3).dropRight(1).toInt
      (id, (stamp, station, stay))
    })

    val personalData = APFile.groupByKey().mapValues(_.toList.sortBy(_._1))

    // 将个人出行划分
    val slice = personalData.flatMap(line => {
      // 设置出行片段长度阈值
      val m = 1
      val MacId = line._1
      val data = line._2
      val segment = new ListBuffer[(Long, String, Int)]
      val segments = new ListBuffer[List[(Long, String, Int)]]
      for (s <- data) {
        if (segment.isEmpty) {
          segment.append(s)
        }
        else {
          // 遇到前后相邻为同一站点进行划分
          if (s._2 == segment.last._2){
            if (segment.length > m) {
              segments.append(segment.toList)
            }
            segment.clear()
          }
          else{
            // 设置容忍时间误差
            var attachInterval = 0
            val odInterval = ODIntervalMap.value((segment.last._2, s._2))
            odInterval / 1800 match {
              case 0 => attachInterval = 600
              case 1 => attachInterval = 1200
              case _ => attachInterval = 1800
            }
            val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
            if (realInterval > odInterval + attachInterval || ( odInterval > 900 && realInterval < odInterval * 0.6 )) {
              if (segment.length > m) {
                segments.append(segment.toList)
              }
              segment.clear()
            }
          }
          segment.append(s)
        }
      }
      if (segment.length > m) {
        segments.append(segment.toList)
      }
      for (s <- segments) yield {
        val stationsList = new ListBuffer[String]
        s.foreach(x => stationsList.append(x._2))
        ((s.head._2, s.last._2), stationsList.toSet)
      }
    })

    val highSampling = slice.filter(x => {
      val minPathLength = validPathMap.value(x._1)._2
      if ( x._2.size - 2 >= (minPathLength - 2) * 0.5 )
        true
      else
        false
    })

    val countRouteRatio = highSampling.groupByKey().flatMap(line => {
      val paths = validPathMap.value(line._1)._1
      val allRoutes = line._2.toList
      val countArray = new Array[Int](paths.length)
      val temp = new ListBuffer[Int]
      allRoutes.foreach(r => {
        for (i <- paths.indices) {
          if (paths(i)._1.toSet.intersect(r).equals(r)){
            temp.append(i)
          }
        }
        if (temp.length == 1)
          countArray(temp.head) += 1
        temp.clear()
      })
      val total = countArray.sum.toFloat
      val res = new Array[String](paths.length)
      for (v <- countArray.indices)
        res(v) = (countArray(v) / total).formatted("%.3f")
      for (i <- paths.indices) yield {
        (line._1, paths(i)._1.length, paths(i)._2, paths(i)._3 + "min", countArray(i).toString + "/" + total.toInt.toString, res(i))
      }
    })

    countRouteRatio.repartition(1).sortBy(_._1).saveAsTextFile(args(0) + "/liutao/RCB/RouteChoiceRatioFromAP")

    sc.stop()

  }
}
