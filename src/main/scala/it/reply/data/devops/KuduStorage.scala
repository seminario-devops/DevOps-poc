package it.reply.data.devops

import it.reply.data.pasquali.Storage
import org.apache.spark.sql.SparkSession

case class KuduStorage(storage : Storage) {

  val spark : SparkSession = null

  def storeRatingsToKudu(filename : String) : Unit = {

    val table = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)

    storage.upsertKuduRows(table, "default.kudu_ratings")
  }
}
