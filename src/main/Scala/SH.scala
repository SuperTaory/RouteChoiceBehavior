import org.apache.spark.sql.SparkSession
import CommonFunctions.{transTimeToTimestamp, transTimeToString}

import scala.collection.mutable.ArrayBuffer

object SH {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("SH").getOrCreate()
        val sc = spark.sparkContext

//        // 602141128,2015-04-01,07:51:08,1号线莘庄,地铁,0.00,非优惠
//        val file = sc.textFile(args(0) + args(1)).map(line => {
//            val fields = line.split(",")
//            val id = fields(0)
//            val time = fields(1) + ' ' + fields(2)
//            val station = fields(3)
//            val tag = fields(4)
//            val ticket = fields(5)
//            (id, time, station, tag, ticket)
//        }).filter(_._4 == "地铁")
//        file.repartition(100).sortBy(x => (x._1, x._2)).saveAsTextFile(args(0) + "SH/Subway")

        // (3102793478,2015-04-01 09:08:54,4号线大木桥路 ,地铁,3.00)
        val file = sc.textFile(args(0) + args(1)).map(line => {
            val fields = line.drop(1).dropRight(1).split(",")
            val id = fields(0)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2).trim
            val ticket = fields(4)
            (id, (time, station, ticket))
        })

//        val groupedData = file.aggregateByKey(new ArrayBuffer[(Long, String, String)]())((u,v) => u+=v, (u1, u2) => u1++u2)
        val groupedData = file.groupByKey()

        val pairRDD = groupedData.flatMap(row => {
            val data = row._2.toArray.sortBy(_._1)
            val pairs = new ArrayBuffer[(String, String, String, String, String, String, String)]()
            var index = 0
            while (index + 1 < data.length) {
                if (data(index)._3.toFloat == 0 & data(index+1)._3.toFloat > 0){
                    val o = data(index)
                    val d = data(index+1)
                    pairs.append((row._1, transTimeToString(o._1), o._2, o._3, transTimeToString(d._1), d._2, d._3))
                    index += 2
                }
            }
            for (pair <- pairs) yield
                pair
        })
        pairRDD.repartition(100).sortBy(x => (x._1, x._2)).saveAsTextFile(args(0) + "SH/SubwayPair")
        sc.stop()
    }
}
