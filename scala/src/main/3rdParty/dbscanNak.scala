import au.com.bytecode.opencsv.CSVWriter
import breeze.linalg.{DenseMatrix, DenseVector}
import nak.cluster._
import nak.cluster.GDBSCAN._
import nak.cluster.Kmeans._
import org.apache.spark.sql.{DataFrame, SparkSession}
import nak.cluster.GDBSCAN._

import java.io.FileWriter
import scala.math.{atan2, cos, sin, sqrt}
import scala.util.matching.Regex

object DbscanNak {
  def main(args: Array[String]) {

    println("Start spark")
    val spark = SparkSession.builder()
      .appName("DBSCAN Example")
      .master("local[*]") // Set the master URL based on your cluster configuration
      .getOrCreate()

    val data: DataFrame = spark.read
      .format("csv")
      .option("header", "true") // Specify if the CSV file has a header row
      .option("inferSchema", "true") // Automatically infer column types
      .load("/Users/nickchien/Downloads/xyDataTop.csv") // Replace with the path to your actual CSV file

    println("xyData.csv data:")
    data.show()

    val eps = 0.05/6371
    val minPoints = 50

    println("param eps:", eps)
    println("param minPoints:", minPoints)

    // x = LONGITUDE
    // y = LATITUDE
    // Convert DataFrame to RDD of (x, y) tuples
    val points = data.select("x", "y")
      .rdd
      .map(row => (row.getDouble(1), row.getDouble(0)))
      .cache() // Cache the RDD for reuse

    val denseMatrix = DenseMatrix(points.collect(): _*)

    def toRadians(degrees: Double): Double = degrees * (Math.PI / 180)

    def haversineDistance(point1: DenseVector[Double], point2: DenseVector[Double]): Double = {
      val lat1 = point1(0)
      val lon1 = point1(1)
      val lat2 = point2(0)
      val lon2 = point2(1)

      val earthRadius = 6371.0 // Earth's radius in kilometers

      val dLat = toRadians(lat2 - lat1)
      val dLon = toRadians(lon2 - lon1)

      val a = sin(dLat / 2) * sin(dLat / 2) +
        cos(toRadians(lat1)) * cos(toRadians(lat2)) *
          sin(dLon / 2) * sin(dLon / 2)

      val c = 2 * atan2(sqrt(a), sqrt(1 - a))

      val distance = earthRadius * c
      distance
    }

    def dbscan(v: breeze.linalg.DenseMatrix[Double]) = {
      val gdbscan = new GDBSCAN(
        DBSCAN.getNeighbours(epsilon = eps, distance = haversineDistance),
        DBSCAN.isCorePoint(minPoints = minPoints)
      )
      val clusters = gdbscan cluster v
      val clusterPoints = clusters.map(_.points.map(_.toString().toArray))

      clusterPoints
    }
    val result = dbscan(denseMatrix)
    // Define the output file path
    val outputFile = "/Users/nickchien/Downloads/output.csv"

    // Create a FileWriter and CSVWriter
    val writer = new CSVWriter(new FileWriter(outputFile))

    // Write the header row
    writer.writeNext(Array("x", "y"))

    // Iterate over the list of points
    result.foreach { point =>
      point.foreach { p =>
        val pattern: Regex = "\\((-?\\d+\\.\\d+), (-?\\d+\\.\\d+)\\)".r

        val result: Option[(Double, Double)] = pattern.findFirstMatchIn(p.mkString).map { matchResult =>
          val lat: Double = matchResult.group(1).toDouble
          val lon: Double = matchResult.group(2).toDouble
          (lat, lon)
        }

        result match {
          case Some((lat, lon)) =>
            writer.writeNext(Array(lat.toString,lon.toString))
            println(s"Latitude: $lat, Longitude: $lon")
          case None =>
            println("No match found")
        }
      }
    }

    // Close the writer
    writer.close()
//    val clustersRdd = checkinsRdd.mapValues(dbscan(denseMatrix))
    spark.stop();
  }
}
