import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession}
import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter
import scala.math._

object DbscanHarversine {

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

    val eps = 5
    val minPoints = 50

    println("param eps:", eps)
    println("param minPoints:", minPoints)

    // x = LONGITUDE
    // y = LATITUDE
    // Convert DataFrame to RDD of (x, y) tuples
    val points = data.select("x", "y")
      .rdd
      .map(row => (row.getDouble(1), row.getDouble(0)))

    def toRadians(degrees: Double): Double = degrees * (Math.PI / 180)

    def haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
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

    def getHaversineNeighbors(point: (Double, Double), points: Iterable[(Double, Double)], eps: Double): Iterable[(Double, Double)] = {
      val (lat1, lon1) = point
      points.filter { case (lat2, lon2) => haversineDistance(lat1, lon1, lat2, lon2) <= eps && point != (lat2, lon2) }
    }

    // Function to get the cluster center point given a list of points
    def calculateClusterCenter(points: Iterable[(Double, Double)]): (Double, Double) = {
      val numPoints = points.size
      val sumX = points.map(_._1).sum
      val sumY = points.map(_._2).sum
      (sumX / numPoints, sumY / numPoints)
    }

    // Function to perform DBSCAN clustering
    def dbscan(points: Iterable[(Double, Double)], eps: Double, minPoints: Int): Set[(Double, Double)] = {
      var clusterId = 0
      val visited = scala.collection.mutable.Set.empty[(Double, Double)]
      val noise = scala.collection.mutable.Set.empty[(Double, Double)]
      val clusters = scala.collection.mutable.Map.empty[(Double, Double), Int]
      val clusterPoints = scala.collection.mutable.Map.empty[Int, scala.collection.mutable.ArrayBuffer[(Double, Double)]]

      def expandCluster(point: (Double, Double), neighbors: Iterable[(Double, Double)]): Unit = {
        clusters(point) = clusterId
        clusterPoints.getOrElseUpdate(clusterId, scala.collection.mutable.ArrayBuffer()) += point

        for (neighbor <- neighbors) {
          if (!visited.contains(neighbor)) {
            visited.add(neighbor)
            val neighborNeighbors = getHaversineNeighbors(neighbor, points, eps)
            if (neighborNeighbors.size >= minPoints) {
              expandCluster(neighbor, neighborNeighbors)
            }
          }
          if (!clusters.contains(neighbor)) {
            clusters(neighbor) = clusterId
            clusterPoints(clusterId) += neighbor
          }
        }
      }

      for (point <- points) {
        if (!visited.contains(point)) {
          visited.add(point)
          val neighbors = getHaversineNeighbors(point, points, eps)
          if (neighbors.size < minPoints) {
            noise.add(point)
          } else {
            clusterId += 1
            expandCluster(point, neighbors)
          }
        }
      }

      val clusterCenters = clusterPoints.mapValues(calculateClusterCenter).values.toSet
      clusterCenters
    }

    val clusters = dbscan(points.collect(), eps, minPoints)

    // Write the cluster centers to a CSV file
    val outputFile = "/Users/nickchien/Downloads/output.csv"
    val writer = new CSVWriter(new FileWriter(outputFile))
    writer.writeNext(Array("x", "y")) // Write the header row

    for (clusterCenter <- clusters) {
      val (x, y) = clusterCenter
      writer.writeNext(Array(x.toString, y.toString))
    }

    writer.close()
    spark.stop()
    println("spark stop")
  }
}
