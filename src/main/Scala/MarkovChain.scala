import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object MarkovChain {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("MarkovChain").getOrCreate()
        val sc = spark.sparkContext

        val file = sc.textFile(args(0) + "/zlt/RCB-2021/IrTripFeatures_SH/part-00000").map(line => {
            val fields = line.split(":")
            val ds = fields(2).toInt
            val data = fields(1).split("#")

            val group = data(0).split(",").map(_.toFloat)

            val indiv = data(2).split(",").map(_.toFloat)

            if (indiv.sum == 0) {
                val newStationNum = new ArrayBuffer[(Int, Float)]()
                for (i <- group.indices){
                    newStationNum.append((i, group(i)))
                }
                val sortNums = newStationNum.sortBy(_._2).reverse
                val part = sortNums.take(args(1).toInt).map(_._1).toSet
                (ds, part)
            } else {
                val arr = new ArrayBuffer[(Int, Float)]()
                for (i <- indiv.indices){
                    arr.append((i, indiv(i)))
                }
                val sortNums = arr.sortBy(_._2).reverse
                val part = sortNums.take(args(1).toInt).map(_._1).toSet
                (ds, part)
            }
        })

        val res = file.map(line => {
            if (line._2.contains(line._1))
                (1, 1)
            else
                (0, 1)
        }).reduceByKey(_+_)
//        val resMap = res.collect().toMap
//        println("****************")
//        println(resMap(1).toFloat / (resMap(0) + resMap(1)))
//        println("****************")

        res.repartition(1).map(x => (x._1, x._2, args(1))).saveAsTextFile(args(0) + "/zlt/RCB-2021/MarkovChain-ST_SH/" + args(1))
        sc.stop()
    }
}
