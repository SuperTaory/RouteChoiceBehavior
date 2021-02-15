import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object RankDistribution {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("RankDistribution").getOrCreate()
        val sc = spark.sparkContext

        // 读取AFC数据 (362138504,2019-06-12 21:49:27,罗湖,21,2019-06-12 22:10:20,黄贝岭,22)
        val rankRDD = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val stations = new ListBuffer[String]
            stations.append(fields(2))
            stations.append(fields(5))
            (id, stations.toList)
        }).groupByKey()
            .mapValues(line => {
                val data = line.reduce(_ ++ _)
                val des = line.map(x => x.last).toArray
                val rank = data.toArray.groupBy(x => x).mapValues(_.length).toArray.sortBy(_._2).reverse.map(_._1)
                (des, rank)
            })

        val res = rankRDD.flatMap(line => {
            val rank = line._2._2
            for (s <- line._2._1) yield {
                (rank.indexOf(s) + 1, 1)
            }
        }).reduceByKey(_ + _).repartition(1).sortByKey().cache()
        val total = res.map(_._2).reduce(_ + _)
        val distribution = res.map(x => (x._1, x._2.toFloat / total))

        distribution.saveAsTextFile(args(0) + "/zlt/RCB/RankDistribution")
        sc.stop()
    }
}
