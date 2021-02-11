import org.apache.spark.sql.SparkSession

object EmpiricalEstimation {
    def main(args: Array[String]): Unit = {
        /**
         * 基于经验估计的目的站点预测
         */
        val spark = SparkSession.builder().appName("EmpiricalEstimation").getOrCreate()
        val sc = spark.sparkContext

        val file = sc.textFile(args(0) + "/zlt/RCB/IrregularTrip_Num/part-00000").map(line => {
            val fields = line.split(":")
            val os = fields(0).toInt
            val ds = fields(2).toInt
            val data = fields(1).split("#")

            val group_day_flow = data(0).split(",").map(_.toFloat)
            val max_gdf = group_day_flow.max
            val argMax_gdf = group_day_flow.indexOf(max_gdf)

            val group_period_flow = data(1).split(",").map(_.toFloat)
            val max_gpf = group_period_flow.max
            val argMax_gpf = group_period_flow.indexOf(max_gpf)
            (ds, argMax_gdf, argMax_gpf)
        }).cache()

        val total = file.count().toFloat
        val acc = file.filter(x => x._1 == x._2 || x._1 == x._3)
        println("**********")
        print(acc.count() / total)
        println("**********")
        sc.stop()
    }
}
