import org.apache.spark.util.AccumulatorV2

class ClusterPointsAccumulator extends AccumulatorV2[(Int, (Double, Double)), Map[Int, scala.collection.mutable.ArrayBuffer[(Double, Double)]]] {
  private val clusterPoints = scala.collection.mutable.Map.empty[Int, scala.collection.mutable.ArrayBuffer[(Double, Double)]]

  override def isZero: Boolean = clusterPoints.isEmpty

  override def copy(): AccumulatorV2[(Int, (Double, Double)), Map[Int, scala.collection.mutable.ArrayBuffer[(Double, Double)]]] = {
    val newAccumulator = new ClusterPointsAccumulator()
    newAccumulator.clusterPoints ++= clusterPoints
    newAccumulator
  }

  override def reset(): Unit = clusterPoints.clear()

  override def add(v: (Int, (Double, Double))): Unit =
    clusterPoints.getOrElseUpdate(v._1, scala.collection.mutable.ArrayBuffer()) += v._2

  override def merge(other: AccumulatorV2[(Int, (Double, Double)), Map[Int, scala.collection.mutable.ArrayBuffer[(Double, Double)]]]): Unit = {
    other.value.foreach { case (clusterId, points) =>
      clusterPoints.getOrElseUpdate(clusterId, scala.collection.mutable.ArrayBuffer()) ++= points
    }
  }

  override def value: Map[Int, scala.collection.mutable.ArrayBuffer[(Double, Double)]] = clusterPoints.toMap
}
