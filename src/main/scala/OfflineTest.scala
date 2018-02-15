import it.reply.data.devops.{BinaryALS, BinaryALSValidator}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object OfflineTest {

  var mr : BinaryALS = null

  def main(args: Array[String]): Unit = {

    mr = BinaryALS().initSpark("test", "local")

    val spark = mr.spark

    val ratings = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("data/ratings.csv")
      .drop("time")
      .rdd.map { rate =>
      Rating(rate(0).toString.toInt, rate(1).toString.toInt, rate(2).toString.toDouble)
    }.cache()


    val Array(train, test) = ratings.randomSplit(Array(0.8, 0.2))

    mr.trainModelBinary(train, 10, 10, 0.1)

    val validator = BinaryALSValidator(mr.model).init(test)

    println(s"MSE = ${validator.MSE}")
    println(s"RMSE = ${validator.RMSE}")
    println(s"accuracy = ${(validator.accuracy*100).toInt}%")
    println(s"precision = ${(validator.precision*100).toInt}%")
    println(s"recall = ${(validator.recall*100).toInt}%")
  }
}
