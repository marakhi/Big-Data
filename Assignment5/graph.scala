import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object graph{
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: graph <input file>  <output file>")
        System.exit(1);
      }
    //Creating a spark context
    val conf = new SparkConf().setAppName("graph app").setMaster("yarn");
    val sc = new SparkContext(conf)
    
    val myrdd = sc.textFile(args(0))
    val myrdd1 = myrdd.map(line=> line.split(" "))
    val edges = myrdd1.map(splits => (splits(0).toLong,splits(1).toLong)).distinct
    val graph=Graph.fromEdgeTuples(edges,null)
    val a = graph.inDegrees
    val c = graph.numVertices
    //System.out.println(c)
    val b = a.map(value => (value._2,1))
    val d = b.reduceByKey((x, y) => x + y)
    val e = d.map(value => (value._1,(value._2.toFloat/c.toFloat)))
    //val e = d.map(value => (value._1,((value._2.toFloat * value._1.toFloat)/c.toFloat)))   
    
    e.saveAsTextFile(args(1));
    sc.stop();
  }
}