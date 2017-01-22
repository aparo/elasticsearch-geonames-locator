import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.elasticsearch.spark.rdd.EsSpark

import scala.util.Try

object GeonameIngester {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("GeonameIngester")
      .getOrCreate()

    import scala.reflect.ClassTag
    implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)

    import sparkSession.implicits._

    val geonameSchema = StructType(Array(
      StructField("geonameid", IntegerType, false),
      StructField("name", StringType, false),
      StructField("asciiname", StringType, true),
      StructField("alternatenames", StringType, true),
      StructField("latitude", FloatType, true),
      StructField("longitude", FloatType, true),
      StructField("fclass", StringType, true),
      StructField("fcode", StringType, true),
      StructField("country", StringType, true),
      StructField("cc2", StringType, true),
      StructField("admin1", StringType, true),
      StructField("admin2", StringType, true),
      StructField("admin3", StringType, true),
      StructField("admin4", StringType, true),
      StructField("population", DoubleType, true), // Asia population overflows Integer
      StructField("elevation", IntegerType, true),
      StructField("gtopo30", IntegerType, true),
      StructField("timezone", StringType, true),
      StructField("moddate", DateType, true)))

    val CITY_CODE = "P"

    val GEONAME_PATH = "downloads/allCountries.txt"


    val geonames = sparkSession.sqlContext.read
      .option("header", false)
      .option("quote", "")
      .option("delimiter", "\t")
      .option("maxColumns", 22)
      .schema(geonameSchema)
      .csv(GEONAME_PATH)
      //      .filter($"fclass"===CITY_CODE)
      //      .filter($"population">0)
      .cache()

    case class GeoPoint(lat: Double, lon: Double)

    case class Geoname(geonameid: Int,
                       name: String,
                       asciiname: String,
                       alternatenames: List[String],
                       latitude: Float,
                       longitude: Float,
                       location: GeoPoint,
                       fclass: String,
                       fcode: String,
                       country: String,
                       cc2: String,
                       admin1: Option[String],
                       admin2: Option[String],
                       admin3: Option[String],
                       admin4: Option[String],
                       population: Double,
                       elevation: Int,
                       gtopo30: Int,
                       timezone: String,
                       moddate: String)



    implicit def emptyToOption(value: String): Option[String] = {
      if (value == null) return None
      val clean = value.trim
      if (clean.isEmpty) {
        None
      } else {
        Some(clean)
      }
    }

    def fixNullInt(value: Any): Int = {
      if (value == null) 0 else {
        Try(value.asInstanceOf[Int]).toOption.getOrElse(0)
      }
    }

    val records = geonames.map {
      row =>
        val id = row.getInt(0)
        val lat = row.getFloat(4)
        val lon = row.getFloat(5)
        Geoname(id, row.getString(1), row.getString(2),
          Option(row.getString(3)).map(_.split(",").map(_.trim).filterNot(_.isEmpty).toList).getOrElse(Nil),
          lat, lon, GeoPoint(lat, lon),
          row.getString(6), row.getString(7), row.getString(8), row.getString(9),
          row.getString(10), row.getString(11), row.getString(12), row.getString(13),
          row.getDouble(14), fixNullInt(row.get(15)), row.getInt(16), row.getString(17), row.getDate(18).toString

        )
    }

    EsSpark.saveToEs(records.toJavaRDD, "geonames/geoname", Map("es.mapping.id" -> "geonameid"))

  }

}
