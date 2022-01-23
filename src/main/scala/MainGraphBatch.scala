import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame


object MainGraphBatch {

  def main(args: Array[String]): Unit = {

    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
    // in order not to show all spark info log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    logger.info("Creating Spark Session")
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("Porcess graph Batch")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    logger.info("spark.sql.shuffle.partitions: " + spark.conf.get("spark.sql.shuffle.partitions"))

    val dataPath = "src/test/scala/data/"
    val nodes = spark.read.option("header", value = true).csv(dataPath + "transport-nodes.csv")
    val directRelsDf = spark.read.option("header", value = true).csv(dataPath + "transport-relationships.csv")
    val reverseDirectRelsDf = directRelsDf
      .withColumn("newSrc", col("dst"))
      .withColumn("newDst", col("src"))
      .drop("dst", "src")
      .withColumnRenamed("newSrc", "src")
      .withColumnRenamed("newDst", "dst")
      .select("src", "dst", "relationship", "cost")

    val relationshipDf = directRelsDf.unionByName(reverseDirectRelsDf)

    val graph = GraphFrame(nodes, relationshipDf)

    graph.vertices.show()
    graph.degrees.show()

    /** find shortest "not weighted" path between two nodes, BFS algorithm */
    val from_expr = "id='Den Haag'"
    val to_expr = "population > 100000 and population < 300000 and id <> 'Den Haag'"
    val bfsResult = graph.bfs
      .fromExpr(from_expr)
      .toExpr(to_expr)
      .run()
    val vertexResult = bfsResult.columns
      .filter(c => !c.startsWith("e"))
      .map(c => col(c))
    bfsResult.select(vertexResult:_*).show(false)

    /** All Pairs Shortest Path */
    val landmarks = Seq("Colchester", "Immingham", "Hoek van Holland")
    val shortestPathsResult = graph.shortestPaths
      .landmarks(landmarks)
      .run()
    shortestPathsResult.show(false)

  }
}
