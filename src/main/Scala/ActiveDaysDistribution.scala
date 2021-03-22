import CommonFunctions.{dayOfMonth_string, hourOfDay}
import org.apache.spark.sql.SparkSession

object ActiveDaysDistribution {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("ActiveDaysDistribution").getOrCreate()
        val sc = spark.sparkContext

        // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
        val dayNum = sc.textFile(args(0) + "/SH/SubwayPair/*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val day = dayOfMonth_string(fields(1))
            (id, day)
        }).groupByKey().map(x => (x._2.toSet.size, 1)).reduceByKey(_ + _).cache()

        val totalNum = dayNum.map(_._2).reduce(_ + _)

        val distribution = dayNum.map(x => (x._1, x._2.toFloat / totalNum)).repartition(1).sortByKey()
        distribution.saveAsTextFile(args(0) + "/zlt/RCB/ActiveDaysDistribution_SH")
        sc.stop()
    }
}
