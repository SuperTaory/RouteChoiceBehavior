import org.apache.spark.sql.SparkSession

object GroupByOT {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("GroupByOT").getOrCreate()
        val sc = spark.sparkContext

        val file = sc.textFile(args(0) + "zlt/RCB/IrTripFeatures/IrTrip_all/part-*").map(line => {
            val fields = line.split(":")
            val ot = fields(3).toInt
            (ot, line)
        }).cache()

        for (i <- 11.until(12)){
            file.filter(_._1 == i).map(_._2).repartition(1)
                .saveAsTextFile(args(0) + "zlt/RCB/IrTripFeatures/trips_ot_" + i.toString)
        }
        sc.stop()
    }
}
