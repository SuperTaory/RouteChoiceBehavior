import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object EmpiricalEstimation {
    def main(args: Array[String]): Unit = {
        /**
         * 基于经验估计的目的站点预测
         */
        val spark = SparkSession.builder().appName("EmpiricalEstimation").getOrCreate()
        val sc = spark.sparkContext

        val file = sc.textFile(args(0) + "/zlt/RCB/IrTripFeatures/IrTrip_all_pec/merged").map(line => {
            val fields = line.split(":")
            val os = fields(0).toInt
            val ds = fields(2).toInt
            val data = fields(1).split("#")

            val stationNum = data(4).split(",").map(_.toInt)
            val newStationNum = new ArrayBuffer[(Int, Int)]()
            for (i <- stationNum.indices){
                newStationNum.append((i, stationNum(i)))
            }
            val sortNums = newStationNum.sortBy(_._2).reverse
            val part = sortNums.take(args(1).toInt).map(_._1).toSet

            (ds, part)
        })

        val res = file.map(line => {
            if (line._2.contains(line._1))
                (1, 1)
            else
                (0, 1)
        }).reduceByKey(_+_).repartition(1).map(x => (x._1, x._2, args(1)))
        res.saveAsTextFile(args(0) + "/zlt/RCB/EmpiricalEstimation/" + args(1))
        sc.stop()
    }
}
