import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Word Count
  */
object WordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark Count"))

    val threshold = 1
    val tokenized = sc.textFile("text").flatMap(_.split(" "))
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with fewer than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    println(charCounts.collect().mkString(", "))
  }
}