import org.apache.spark.sql.SparkSession

object TaxiFlow {
    def main(args: Array[String]): Unit = {
        /**
         * 统计深圳市某grid内taxi的流入和流出流量
         */
        val spark = SparkSession.builder()
            .appName("TaxiFlow")
            .getOrCreate()
        val sc = spark.sparkContext

        // 001A0A6EBF1BD107,2016-06-26 11:51:12,113.813515,22.623617,0,1
        // 读取taxi的gps的记录
        val file = sc.textFile(args + "/SZODA/taxi/gps/2016-04-*.gz")
    }
}
