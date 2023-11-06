import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math._

object DbscanHaversine2 {
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
      .cache() // Cache the RDD for reuse

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

    def getHaversineNeighbors(point: (Double, Double), points: Broadcast[Array[(Double, Double)]], eps: Double): Array[(Double, Double)] = {
//      val broadcastEps: Broadcast[Double] = spark.sparkContext.broadcast(eps)

      points.value.filter { case (lat2, lon2) =>
        val (lat1, lon1) = point
        val distance = haversineDistance(lat1, lon1, lat2, lon2)
        distance <= eps && point != (lat2, lon2)
      }
    }

    // Function to get the cluster center point given a list of points
    def calculateClusterCenter(points: Iterable[(Double, Double)]): (Double, Double) = {
      val numPoints = points.size
      val sumX = points.map(_._1).sum
      val sumY = points.map(_._2).sum
      (sumX / numPoints, sumY / numPoints)
    }

    // Function to perform DBSCAN clustering
    def dbscan(points: RDD[(Double, Double)], eps: Double, minPoints: Int): Set[(Double, Double)] = {
      var clusterId = 0
      val visited = scala.collection.mutable.Set.empty[(Double, Double)]
      val noise = scala.collection.mutable.Set.empty[(Double, Double)]
      val clusters = scala.collection.mutable.Map.empty[(Double, Double), Int]
      val clusterPointsAccumulator = new ClusterPointsAccumulator
      points.sparkContext.register(clusterPointsAccumulator, "clusterPointsAccumulator")
      val broadcastPoints: Broadcast[Array[(Double, Double)]] = points.sparkContext.broadcast(points.collect())

      def expandCluster(point: (Double, Double)): Unit = {
        clusters(point) = clusterId
        clusterPointsAccumulator.add((clusterId, point))

        val neighbors = getHaversineNeighbors(point, broadcastPoints, eps)
        neighbors.foreach { neighbor =>
          if (!visited.contains(neighbor)) {
            visited.add(neighbor)
            val neighborNeighbors = getHaversineNeighbors(neighbor, broadcastPoints, eps)
            if (neighborNeighbors.length >= minPoints) {
              expandCluster(neighbor)
            }
          }
          if (!clusters.contains(neighbor)) {
            clusters(neighbor) = clusterId
            clusterPointsAccumulator.add((clusterId, neighbor))
          }
        }
      }

      points.mapPartitions { iter =>
        iter.foreach { point =>
          if (!visited.contains(point)) {
            visited.add(point)
            val neighbors = getHaversineNeighbors(point, broadcastPoints, eps)
            if (neighbors.length >= minPoints) {
              expandCluster(point)
              clusterId += 1
            } else {
              noise.add(point)
            }
          }
        }
        iter
      }.count()

      // Retrieve the cluster points from the accumulator
      val clusterPoints = clusterPointsAccumulator.value

      println("clusterPoints: ", clusterPoints.size)
      // Get the cluster center points
      val clusterCenters = clusterPoints.map { case (_, cluster) => calculateClusterCenter(cluster) }.toSet

      // Print the results
      println("Clusters:")
      clusterCenters.foreach(println)

      println("Noise:")
      noise.foreach(println)

      clusterCenters
    }

    val clusterCenters = dbscan(points, eps, minPoints)
    println("Number of clusters:", clusterCenters.size)

    // Save the cluster centers to a CSV file
    val writer = new FileWriter("/Users/nickchien/Downloads/clusterCenters.csv") // Replace with your desired file path
    val csvWriter = new CSVWriter(writer)
    val header = Array("Latitude", "Longitude")
    csvWriter.writeNext(header)
    clusterCenters.foreach { case (lat, lon) =>
      val row = Array(lat.toString, lon.toString)
      csvWriter.writeNext(row)
    }
    csvWriter.close()

    spark.stop()
  }
}
