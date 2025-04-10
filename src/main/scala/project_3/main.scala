package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.util.Random

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((_, _) => 0) // 0 = undecided, 1 = in MIS, -1 = excluded
    var progressMade = true
    var iteration = 0

    val startTime = System.currentTimeMillis()

    while (progressMade) {
      iteration += 1
      val iterStart = System.currentTimeMillis()

      // Assign random priority to undecided vertices
      val randomGraph = g.mapVertices { case (_, attr) =>
        if (attr == 0) Random.nextDouble() else -1.0
      }

      // Each vertex receives max neighbor priority
      val neighborMax = randomGraph.aggregateMessages[Double](
        triplet => {
          if (triplet.srcAttr > 0 && triplet.dstAttr > 0) {
            triplet.sendToDst(triplet.srcAttr)
            triplet.sendToSrc(triplet.dstAttr)
          }
        },
        math.max
      )

      // Select vertices that have highest priority among neighbors
      val newMIS = randomGraph.vertices.leftJoin(neighborMax) {
        case (_, priority, maxNeighborPriorityOpt) =>
          val maxNeighborPriority = maxNeighborPriorityOpt.getOrElse(-1.0)
          priority > maxNeighborPriority
      }.filter(_._2).mapValues(_ => 1) // mark selected vertices with 1

      // Update vertices: add new MIS vertices
      g = g.joinVertices(newMIS) {
        case (_, oldAttr, newAttr) => newAttr
      }

      // Deactivate neighbors of new MIS vertices
      val neighborDeactivations = g.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0) triplet.sendToDst(-1)
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0) triplet.sendToSrc(-1)
        },
        (a, _) => a
      )

      g = g.outerJoinVertices(neighborDeactivations) {
        case (_, oldAttr, Some(update)) if oldAttr == 0 => update
        case (_, oldAttr, _) => oldAttr
      }

      val activeVertices = g.vertices.filter(_._2 == 0).count()
      val iterTime = (System.currentTimeMillis() - iterStart) / 1000.0

      println(s"""
        |Iteration $iteration:
        |  - Time: $iterTime seconds
        |  - Active vertices remaining: $activeVertices
        |  - New MIS vertices: ${newMIS.count()}
        |""".stripMargin)

      progressMade = newMIS.count() > 0
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    val isValid = verifyMIS(g)

    println(s"""
      |=== Final Summary ===
      |Total iterations: $iteration
      |Total runtime: $totalTime seconds
      |Valid MIS: $isValid
      |===================
      |""".stripMargin)

    g
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
