package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    // Initialize all vertices with 0 (undecided)
    var g = g_in.mapVertices((id, attr) => 0)
    
    // Keep track of active vertices (not yet decided)
    var activeGraph = g.mapVertices((id, attr) => true)
    var remainingVertices = activeGraph.vertices.filter(_._2 == true).count()
    
    // Track number of iterations
    var iterations = 0
    
    while (remainingVertices > 0) {
      iterations += 1
      
      // Assign random priorities to active vertices
      val randomGraph = activeGraph.mapVertices((id, attr) => 
        if (attr) scala.util.Random.nextDouble() else -1.0)
      
      // Find vertices with higher priority than all their neighbors
      val messageGraph = randomGraph.aggregateMessages[Double](
        triplet => {
          if (triplet.srcAttr > 0 && triplet.dstAttr > 0) {
            if (triplet.srcAttr > triplet.dstAttr) {
              triplet.sendToDst(triplet.srcAttr)
            } else {
              triplet.sendToSrc(triplet.dstAttr)
            }
          }
        },
        math.max
      )
      
      // Identify vertices to add to MIS (higher priority than all neighbors)
      val misUpdates = randomGraph.vertices.leftJoin(messageGraph) {
        case (id, priority, maxNeighborOpt) =>
          val maxNeighbor = maxNeighborOpt.getOrElse(-1.0)
          priority > 0 && (maxNeighbor == -1.0 || priority > maxNeighbor)
      }
      
      // Add selected vertices to MIS
      val newMisVertices = misUpdates.filter(_._2 == true).map(_._1).collect()
      
      // Update graph: mark selected vertices as in MIS (1) and their neighbors as not in MIS (-1)
      val newGraph = g.mapVertices((id, attr) => 
        if (newMisVertices.contains(id) && attr == 0) 1 else attr
      )
      
      // Mark neighbors of MIS vertices as not in MIS
      val neighborUpdates = newGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0) {
            triplet.sendToDst(-1)
          }
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0) {
            triplet.sendToSrc(-1)
          }
        },
        (a, b) => -1
      )
      
      // Apply neighbor updates
      g = newGraph.joinVertices(neighborUpdates)((id, oldAttr, newAttr) => 
        if (oldAttr == 0) newAttr else oldAttr
      )
      
      // Update active graph: vertices are active if they're still undecided
      activeGraph = g.mapVertices((id, attr) => attr == 0)
      remainingVertices = activeGraph.vertices.filter(_._2 == true).count()
    }
    
    // Make sure the result is a valid MIS
    val isValidMIS = verifyMIS(g)
    println(s"Luby's algorithm completed in $iterations iterations. Valid MIS: $isValidMIS")
    
    return g
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // Checks if any adjacent vertices are in the MIS
    val adjacencyCheck = g_in.triplets.filter(triplet => triplet.srcAttr == 1 && triplet.dstAttr == 1).count() > 0
    if (adjacencyCheck) {
      return false
    }
    
    // Checks if for every vertex, it has at least one vertex in its neightborhood in the MIS
    val neighborMIS = g_in.aggregateMessages[Int](
      triplet => {
        if (triplet.srcAttr == 1) triplet.sendToDst(1)
        if (triplet.dstAttr == 1) triplet.sendToSrc(1)
      }, (a,b) => a + b
    )

    val nonMaximal = g_in.vertices.leftJoin(neighborMIS) {
      case (_, label, neighborCount) =>
        label == -1 && neighborCount.getOrElse(0) == 0
      }.filter { case (_, isNonMaximal) => isNonMaximal}
        .count() > 0

      !nonMaximal
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
