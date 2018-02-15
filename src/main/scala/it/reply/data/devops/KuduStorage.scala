package it.reply.data.devops

import it.reply.data.pasquali.Storage
import org.apache.spark.sql.Row

case class KuduStorage(storage : Storage) {

  def storeRatingsToKudu(filename : String) : Unit = {

    val spark = storage.spark

    import spark.implicits._

    val table = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)
      .map{ case Row(userID, movieID, rating, time) =>
        Row(userID.asInstanceOf[Long],
          movieID.asInstanceOf[Long],
          rating.asInstanceOf[Double], time)}.toDF("userid", "movieid", "rating", "time")

    storage.upsertKuduRows(table, "default.kudu_ratings")
  }
}
