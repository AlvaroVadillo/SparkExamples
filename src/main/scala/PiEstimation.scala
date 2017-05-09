import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Pi estimation using Montecarlo's method.
  */
object PiEstimation {

  //We will generate 100.000 numbers
  val NUM_SAMPLES = 100000;

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark Count"))

    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x*x + y*y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
  }
}
