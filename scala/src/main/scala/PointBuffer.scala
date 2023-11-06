// Define a custom buffer class
class PointBuffer(bufferSize: Int) extends Iterator[(Double, Double)] {
  private val buffer = new Array[(Double, Double)](bufferSize)
  private var nextIndex = 0
  private var lastIndex = 0

  override def hasNext: Boolean = nextIndex < lastIndex

  override def next(): (Double, Double) = {
    val point = buffer(nextIndex)
    nextIndex += 1
    point
  }

  def add(point: (Double, Double)): Unit = {
    buffer(lastIndex) = point
    lastIndex += 1
  }
}
