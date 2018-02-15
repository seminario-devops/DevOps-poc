package it.reply.data.devops

import it.reply.data.pasquali.Storage

case class KuduStorage(storage : Storage) {

  def storeRatingsToKudu(filename : String) : Unit = {

    val table = storage.spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filename)

    storage.upsertKuduRows(table, "default.kudu_ratings")
  }
}
