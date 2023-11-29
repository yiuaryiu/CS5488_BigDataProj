// Define a custom buffer class
class PointBuffer(bufferSize: Int) extends Iterator[(Double, Double)] {
  private val buffer = new Array[(Double, Double)](bufferSize)
  private var nextIndex = 0
  private var lastIndex = 0

  // Check if there is a next element in the buffer
  override def hasNext: Boolean = nextIndex < lastIndex

  // Get the next element in the buffer and move the index forward
  override def next(): (Double, Double) = {
    val point = buffer(nextIndex)
    nextIndex += 1
    point
  }

  // Add a new point to the buffer
  def add(point: (Double, Double)): Unit = {
    buffer(lastIndex) = point
    lastIndex += 1
  }
}
