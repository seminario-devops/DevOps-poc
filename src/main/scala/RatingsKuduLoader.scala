import it.reply.data.devops.KuduStorage
import it.reply.data.pasquali.Storage

object RatingsKuduLoader {

  def main(args: Array[String]): Unit = {

    if(args.length != 2)
      println("RatingsKuduLoader kuduaddr kuduport")

    val storage = Storage()
      .init("local", "kudu", false)
      .initKudu(args(0), args(1), "impala::")

    KuduStorage(storage).storeRatingsToKudu("data/ratings.csv")
  }

}
