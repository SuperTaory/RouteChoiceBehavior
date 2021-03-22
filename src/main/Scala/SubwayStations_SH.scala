import org.apache.spark.sql.SparkSession

object SubwayStations_SH {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("SubwayStations_SH").getOrCreate()
        val sc = spark.sparkContext

        // (1,2015-04-02 16:03:21,4号线大连路,地铁,0.00)
        val files = sc.textFile(args(0) + "SH/Subway/part-*").map(line => {
            val fields = line.split(",")
            val station = fields(2).trim
            station
        })
        val stations = files.distinct().repartition(1).sortBy(x => x).zipWithIndex().map(x=>(x._2,x._1))
        stations.saveAsTextFile(args(0) + "SH/SubwayInfo")
        sc.stop()
    }
}
