import org.apache.spark.sql.SparkSession
import CommonFunctions.{transTimeToTimestamp, dayOfWeek, monthOfYear, quarterOfDay, dayOfMonth_long}

object GenerateDataForPrediction {

//  case class Trip_GPS(id:String, ot:Long, dt:Long, os_lon:String, os_lat:String, ds_lon:String, ds_lat:String,
//                  day_of_week:Int, quarter:Int, mon:Int, holiday:Int)
  case class Trip_NUM(id:String, ot:Long, dt:Long, os_num:Int, ds_num:Int, day_of_week:Int, quarter:Int, mon:Int, holiday:Int)

  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder()
      .appName("data for prediction")
      .getOrCreate()
    val sc = spark.sparkContext

    // 2019/6月份假期
    val holidays = Set(7,8,9)
    import spark.implicits._

    /**
     * 处理AFC数据，提取trip特征，提供给destination predictor
     */

    // 读取站点信息 : 1,机场东,22.647011,113.8226476,1268036000,268
    val stationInfo = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt").map(line => {
      val fields = line.split(",")
      val num = fields.head.toInt - 1
      val name = fields(1)
      val lon = fields(3)
      val lat = fields(2)
      (name, num, lon, lat)
    }).cache()
    val stationGPSMap = sc.broadcast(stationInfo.map(x => (x._1, (x._3, x._4))).collect().toMap)
    val stationNumMap = sc.broadcast(stationInfo.map(x => (x._1, x._2)).collect().toMap)

    // 读取AFC数据 (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
    val data_afc = sc.textFile(args(0) + "/Destin/subway-pair/" + args(1))
      .map(line => (line.split(',').head, line))
      .groupByKey()
      .mapValues(_.toList.sorted)
      .filter(_._2.length > 10) // 过滤出行次数
      .flatMap(line => for (trip <- line._2) yield trip)
      .map(line => {
        val fields = line.drop(1).dropRight(1).split(",")
        val id = fields.head
        val ot = transTimeToTimestamp(fields(1))
        val os = fields(2)
        val os_gps = stationGPSMap.value(os)
        val os_num = stationNumMap.value(os)

        val dt = transTimeToTimestamp(fields(4))
        val ds = fields(5)
        val ds_gps = stationGPSMap.value(ds)
        val ds_num = stationNumMap.value(ds)

        val day_of_week = dayOfWeek(ot)
        val quarter = quarterOfDay(ot)
        val month = monthOfYear(ot)
        var holiday = 0
        if (holidays.contains(dayOfMonth_long(ot))) holiday = 1


        Trip_NUM(id, ot, dt, os_num, ds_num, day_of_week, quarter, month, holiday)
//      Trip_GPS(id, ot, dt, os_gps._1, os_gps._2, ds_gps._1, ds_gps._2, day_of_week, quarter, month, holiday)
    }).toDF()

    data_afc.coalesce(1).write.option("header", "true").csv(args(0) + "/liutao/RCB/TripForPrediction/trip_count_10")

    sc.stop()

  }
}
