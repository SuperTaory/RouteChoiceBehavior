import org.apache.spark.sql.SparkSession

object SameInMultiOut {
    def main(args: Array[String]): Unit = {
        /**
         * 统计从一个站点出发到其他站点的出行数量分布
         */
        val spark = SparkSession.builder()
            .appName("DataForDesPrediction")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取AFC数据
        val inOut = sc.textFile(args(0) + "/Destin/subway-pair/*").map(line => {
            val fields = line.split(',')
            val in = fields(2)
            val out = fields(5)
            (in, out)
        })

        //  统计每个站点作为进站时的出行次数以及出站站点的集合大小
        val stat = inOut.groupBy(x => x).mapValues(_.size)

        val res = stat.repartition(1).sortBy(x => (x._1._1, x._2), ascending = false)

        res.saveAsTextFile(args(0) + "/liutao/RCB/SameInMultiOut")

        sc.stop()
    }
}
