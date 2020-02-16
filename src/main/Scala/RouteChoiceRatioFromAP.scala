import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object RouteChoiceRatioFromAP {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Route Choice Ratio From AP").getOrCreate()
    val sc = spark.sparkContext

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
      // 仅保留站点编号信息
      val fields = line.split(' ').dropRight(5)
      val sou = stationNoToName.value(fields(0).toInt)
      val des = stationNoToName.value(fields(fields.length-1).toInt)
      val pathStations = new ListBuffer[String]
      fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
      ((sou, des), pathStations.toList)
    }).groupByKey().mapValues(_.toList)

    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
    val validPathMap = sc.broadcast(validPathFile.collect().toMap)

    // 读取AP数据 (1C151FEE918E,1561006439,灵芝,379)
    val APFile = sc.textFile(args(0) + "/liutao/MacData/part*").map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val stamp = fields(1).toLong
      val station = fields(2)
      val stay = fields(3).dropRight(1).toInt
      (id, stamp, station, stay)
    })


  }
}
