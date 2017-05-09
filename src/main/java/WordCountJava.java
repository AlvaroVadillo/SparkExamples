import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
/**
 * Created by vadi on 5/9/17.
 */
public class WordCountJava {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("src/resources/text");
        JavaPairRDD<String, Integer> counts = input
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);


        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
    }
}
