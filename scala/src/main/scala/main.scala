//import org.apache.spark.ml.clustering.KMeans
//import org.apache.spark.ml.evaluation.ClusteringEvaluator
//import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession}
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.functions.{coalesce, col, collect_list, monotonically_increasing_id, udf}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.{SparkSession, DataFrame}
//import org.apache.spark.ml.linalg.Vectors
import scala.collection.mutable.ListBuffer
import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter
import scala.math._

object Main {

  def main(args: Array[String]) {

    println("hereeee Start spark")
    val spark = SparkSession.builder()
      .appName("DBSCAN Example")
      .master("local[*]") // Set the master URL based on your cluster configuration
      .getOrCreate()

    val data: DataFrame = spark.read
      .format("csv")
      .option("header", "true") // Specify if the CSV file has a header row
      .option("inferSchema", "true") // Automatically infer column types
      .load("/Users/nickchien/Downloads/xyDataTop.csv") // Replace with the path to your actual CSV file

    println("hereeee xyData.csv data: ", data)

    val eps = 0.05/6371
    val minPoints = 50

    println("hereeee param eps:", eps)
    println("hereeee param minPoints:", minPoints)

    // Convert DataFrame to an Array of (x, y) tuples
    val points = data.select("x", "y")
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))
      .collect()

    println("hereeee param points:", points)

    // Function to calculate the Euclidean distance between two points
    def distance(point1: (Double, Double), point2: (Double, Double)): Double = {
      val dx = point1._1 - point2._1
      val dy = point1._2 - point2._2
      sqrt(dx * dx + dy * dy)
    }

    // Function to retrieve the neighbors of a point within a given distance
    def getNeighbors(point: (Double, Double), points: Array[(Double, Double)], eps: Double): Array[(Double, Double)] = {
      points.filter(other => distance(point, other) <= eps && point != other)
    }

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

    def getHaversineNeighbors(point: (Double, Double), points: Array[(Double, Double)], eps: Double): Array[(Double, Double)] = {
      val (lat1, lon1) = point
      points.filter { case (lat2, lon2) => haversineDistance(lat1, lon1, lat2, lon2) <= eps && point != (lat2, lon2) }
    }

    // Function to get the cluster center point given a list of points
    def getClusterCenter(points: Seq[(Double, Double)]): (Double, Double) = {
      val numPoints = points.length
      val sumX = points.map(_._1).sum
      val sumY = points.map(_._2).sum
      (sumX / numPoints, sumY / numPoints)
    }

    // Function to perform DBSCAN clustering and return cluster center points
    def dbscan(points: Array[(Double, Double)], eps: Double, minPoints: Int): Array[(Double, Double)] = {
      val visited = Array.fill(points.length)(false)
      val clusterCenters = scala.collection.mutable.ListBuffer.empty[(Double, Double)]

      val pointIndices = points.zipWithIndex.toMap
      def expandCluster(point: (Double, Double), neighbors: Array[(Double, Double)], neighborIndices: Array[Int]): Unit = {
        visited(pointIndices(point)) = true
        val clusterPoints = scala.collection.mutable.ListBuffer.empty[(Double, Double)] // Initialize cluster points with the current point

        var i = 0
        while (i < neighbors.length) {
          val neighbor = neighbors(i)
          val neighborIndex = pointIndices(neighbor)
          if (!visited(neighborIndex)) {
            visited(neighborIndex) = true
            val neighborNeighbors = getHaversineNeighbors(neighbor, points, eps)
            if (neighborNeighbors.length >= minPoints) {
              neighborNeighbors.foreach { n =>
                if (!clusterPoints.contains(n)) {
                  clusterPoints += n
                }
              }
            }
          }
          i += 1
        }

        if (clusterPoints.nonEmpty) {
          clusterCenters += getClusterCenter(clusterPoints)
        }
      }


      for (i <- points.indices) {
        val point = points(i)
        if (!visited(i)) {
          visited(i) = true
          val neighbors = getHaversineNeighbors(point, points, eps)
          if (neighbors.length >= minPoints) {
            val neighborIndices = neighbors.map(n => pointIndices(n))
            clusterCenters += getClusterCenter(point +: neighbors)
            expandCluster(point, neighbors, neighborIndices)
          }
        }
      }

      clusterCenters.toArray
    }


    // Perform DBSCAN clustering
    val clusterCenters = dbscan(points, eps, minPoints)

    // Set the path where you want to save the CSV file
    val outputPath = "/Users/nickchien/Downloads/output.csv"
    println("hereeee outputPath: ", outputPath)

    // Create a FileWriter and CSVWriter
    val fileWriter = new FileWriter(outputPath)
    val csvWriter = new CSVWriter(fileWriter)

    // Write the header
    val header = Array("x", "y")
    csvWriter.writeNext(header)

    // Write each cluster center point
    clusterCenters.foreach { center =>
      val rowData = Array(center._1.toString, center._2.toString)
      csvWriter.writeNext(rowData)
    }

    // Close the writers
    csvWriter.close()
    fileWriter.close()

    spark.stop()
    println("hereeee Stop spark")

    // Display the results
//    val result = points.zip(clusters)
//      .map { case ((x, y), cluster) => (x, y, cluster) }
//      .toSeq.toDF("x", "y", "cluster")
//
//    result.show()

//    val data = spark.read
//      .format("libsvm")
//      .load("/Users/nickchien/Downloads/xyData.csv") // Replace with the path to your actual data file
//
//    val parsedData = data.rdd.map(row => Vectors.dense(row.getAs[org.apache.spark.ml.linalg.Vector]("features").toArray))
//
//    // Define the DBSCAN parameters
//    val eps = 0.5 // Epsilon (neighborhood distance)
//    val minPoints = 5 // Minimum number of points to form a dense region
//
//    // Custom implementation of DBSCAN
//    def dbscan(data: RDD[org.apache.spark.ml.linalg.Vector], eps: Double, minPoints: Int): RDD[(Int, org.apache.spark.ml.linalg.Vector)] = {
//      // Perform DBSCAN algorithm here
//      // Implement your own logic to cluster the data points
//      // Return a pair of cluster label and corresponding data point
//
//      // Example code:
//      val clusteredData = data.map((0, _)) // Assign all points to a single cluster for demonstration purposes
//      clusteredData
//    }
//
//    val model = dbscan(parsedData, eps, minPoints)
//
//    model.foreach(println)




//    val spark = SparkSession.builder()
//      .appName("DBSCAN Example")
//      .master("local[*]") // Set the master URL based on your cluster configuration
//      .getOrCreate()
//
//    // Loads data.
//    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")
//
//    // Trains a k-means model.
//    val kmeans = new KMeans().setK(2).setSeed(1L)
//    val model = kmeans.fit(dataset)
//
//    // Make predictions
//    val predictions = model.transform(dataset)
//
//    // Evaluate clustering by computing Silhouette score
//    val evaluator = new ClusteringEvaluator()
//
//    val silhouette = evaluator.evaluate(predictions)
//    println(s"Silhouette with squared euclidean distance = $silhouette")
//
//    // Shows the result.
//    println("Cluster Centers: ")
//    model.clusterCenters.foreach(println)
  }
}
