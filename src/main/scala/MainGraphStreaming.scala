import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.graphframes.GraphFrame


object MainGraphStreaming {

  def main(args: Array[String]): Unit = {

    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
    // in order not to show all spark info log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    logger.info("Creating Spark Session")
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("Porcess graph Streaming")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    logger.info("spark.sql.shuffle.partitions: " + spark.conf.get("spark.sql.shuffle.partitions"))

    val dataPath = "src/test/scala/data/workfrance"
    // src,dst,relationship,cost
    val schemaRelations = StructType(
      Array(StructField("from",StringType), StructField("to",StringType), StructField("mins",IntegerType))
    )
    // Stream DFs
    //val nodes = spark.readStream.option("header", value = true).csv(dataPath + "transport-nodes.csv")
    val directRelsDf = spark.readStream.option("header", value = true).schema(schemaRelations).csv(dataPath)

    /** Some metrics */
    val dfMaxCostRoute = directRelsDf
      .withColumn("datetime" ,from_unixtime(unix_timestamp(current_timestamp(), "yyyyMMdd'T'HHmmss:SSSSSS")) )
      .groupBy(window(col("datetime"), "5 seconds"))
      .max("mins")

    val query = dfMaxCostRoute
      .writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
    query.awaitTermination()

    /** Graph operations */
    //val reverseDirectRelsDf = directRelsDf
    //  .withColumn("newSrc", col("dst"))
    //  .withColumn("newDst", col("src"))
    //  .drop("dst", "src")
    //  .withColumnRenamed("newSrc", "src")
    //  .withColumnRenamed("newDst", "dst")
    //  .select("src", "dst", "relationship", "cost")
//
    //val relationshipDf = directRelsDf.unionByName(reverseDirectRelsDf)
//
    //val graph = GraphFrame(nodes, relationshipDf)
//
    //graph.vertices.show()
    //graph.degrees.show()
//
    ///** find shortest "not weighted" path between two nodes, BFS algorithm */
    //val from_expr = "id='Den Haag'"
    //val to_expr = "population > 100000 and population < 300000 and id <> 'Den Haag'"
    //val bfsResult = graph.bfs
    //  .fromExpr(from_expr)
    //  .toExpr(to_expr)
    //  .run()
    //val vertexResult = bfsResult.columns
    //  .filter(c => !c.startsWith("e"))
    //  .map(c => col(c))
    //bfsResult.select(vertexResult:_*).show(false)
//
    ///** All Pairs Shortest Path */
    //val landmarks = Seq("Colchester", "Immingham", "Hoek van Holland")
    //val shortestPathsResult = graph.shortestPaths
    //  .landmarks(landmarks)
    //  .run()
    //shortestPathsResult.show(false)

  }
}
