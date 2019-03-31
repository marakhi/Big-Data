import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.hadoop.conf._ 
import org.apache.hadoop.io._ 
import org.apache.hadoop.mapreduce.lib.input._ 
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//@transient val conf = new Configuration 
//conf.set("textinputformat.record.delimiter","#*")
import org.apache.spark.graphx._
import scala.util.MurmurHash

object finalproject{
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: finalproject <input file>  <output file>")
        System.exit(1);
      }
    
    @transient val conf = new Configuration 
    conf.set("textinputformat.record.delimiter","#*")

    val conf1 = new SparkConf().setAppName("ratings app").setMaster("local")
    val sc = new SparkContext(conf1)
    conf.set("spark.sql.warehouse.dir", "hdfs://bd-hmd05:9000/user/hive/warehouse")
    val spark = SparkSession.builder().master("yarn").getOrCreate()
    spark.conf.set("spark.sql.warehouse.dir", "hdfs://bd-hmd05:9000/user/hive/warehouse")
    
    
    //val conf1 = new SparkConf().setAppName("finalproject app").setMaster("local[2]");
    //val sc = new SparkContext(conf1)
    val inputrdd = sc.newAPIHadoopFile(args(0),classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf).map{case(key,value)=>value.toString}.filter(value=>value.length!=0)
    val mytitle1 = inputrdd.map(line => ((line.split("\n#index",2), (line.split("\n")(0))))).map(line => if (line._1.size > 1)(line._1(1).split("\n")(0).toString(), line._2.toString()) else (" "," ")).filter(x => x != (" "," "))
    val mytitle = mytitle1.map(line => (line._1,line._2))
    
    //.map(line => if (line._1.size > 1)(line._1(1), line._2) else (" ")).filter(x => x != (" "))
 
    
    val myrdd1 = inputrdd.map(line => line.split("\n#!")(0)).map(line => line.split("\n#index")).map(line => if (line.size > 1)(line(1)) else (" ")).filter(x => x != (" ")).map(line => line.split("\n#%",2)).map(line => if (line.size > 1)(line(0),line(1)) else (" "," "))

    val myrdd10 = myrdd1.filter(x => x != (" "," "))
    val myrdd12 = myrdd10.flatMapValues { x => x.split("\n#%")}
    val myrdd2 = myrdd12.map(line =>(line._1, line._2.split("\n")(0)))
    val myrdduniq = myrdd2.map(x => x._1+","+x._2)
    val myrdduniq1 = myrdduniq.flatMap(x => x.split(","))
    val myrdduniq2 = myrdduniq1.map(x => (MurmurHash.stringHash(x), x.toString()))
    val myrdduniq3 = myrdduniq2.distinct()

    val myrdd3 = myrdd2.map(x => ((MurmurHash.stringHash(x._1).toString).toLong,MurmurHash.stringHash(x._2.toString).toLong))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val graph=Graph.fromEdgeTuples(myrdd3,null).partitionBy(PartitionStrategy.RandomVertexCut)
    val node = graph.vertices.keys
    
    val a = graph.inDegrees
    val table2 = a.toDF()
    table2.createOrReplaceTempView("indegree")
    
    val B = graph.outDegrees
    val table3 = B.toDF()
    table3.createOrReplaceTempView("outdegree")
    
    val D = 0.85
    val c = graph.numVertices
    var ranks= node.map{x => (x, (1.0/c.toFloat).toFloat)}
    val table4 = ranks.toDF()
    table4.createOrReplaceTempView("ranks")
    
    val table1 = myrdd3.toDF()
    table1.createOrReplaceTempView("link")
    //val table1 = myrdd3.toDF()
    //val data = spark.sql("select c._2, d._2, a._2, b._1, b._2, e._2, f._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 ")
    //val data = spark.sql("select sum(c._2), sum(d._2), b._1, b._2, e._2, f._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b._1, b._2, e._2, f._2")
    //val data = spark.sql("select e._2/sum(c._2), f._2/sum(d._2), b._1, b._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b._1, b._2, e._2, f._2")
    //val data = spark.sql("select (t.a * r._2), t.b, t.c from (select ((e._2/sum(c._2)) * (f._2/sum(d._2))) as a, b._1 as b, b._2 as c from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b, c, e._2, f._2) tab t, ranks r where t.b = r._1")
    //val data = spark.sql("select ((e._2/sum(c._2)) * (f._2/sum(d._2))), b._1, b._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b._1, b._2, e._2, f._2")
    //val data = spark.sql("select (tab.a * r._2), tab._1, tab._2 from (select ((e._2/sum(c._2)) * (f._2/sum(d._2))) as a, b._1, b._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b._1, b._2, e._2, f._2) tab, ranks r where tab._1 = r._1")

    for (x <-1 to 10){
    implicit def doubleToFloat(d: Double): Float = d.toFloat
    implicit def doubleFunc2floatFunc(df : Double => Float) : Float => Float = (f : Float) => df(f)
    
    val data = spark.sql("select tab1._2, sum(a) from (select (tab.a * r._2) as a, tab._1, tab._2 from (select ((e._2/sum(c._2)) * (f._2/sum(d._2))) as a, b._1, b._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b._1, b._2, e._2, f._2) tab, ranks r where tab._1 = r._1) tab1 group by tab1._2")
    //val data = spark.sql("select ((e._2/sum(c._2)) * (f._2/sum(d._2))) as a, b._1, b._2 from link a, link b, indegree c, outdegree d, indegree e, outdegree f  where a._1 = b._1 and a._2 = c._1 and b._2 = e._1 and a._2 = d._1 and b._2 = f._1 group by b._1, b._2, e._2, f._2")
    //data.take(1000).foreach(println)
    val data1 = data.rdd
    val data2 = data1.map(row=>(row.getAs[Long](0), row.getAs[Double](1)))
    ranks = data2.map(x => (x._1,(x._2)*D + ((1.0-D.toFloat)/c.toFloat)))
    val table4 = ranks.toDF()
    table4.createOrReplaceTempView("ranks")
        }
    
    val ranks1 = ranks.map(x => (x._1.toInt,x._2.toString()))
    val a1 = a.map(x => (x._1.toInt, (x._2.toString(), ((1.0-D.toFloat)/c.toFloat).toFloat)))
    val inlinks = ranks1.rightOuterJoin(a1)
    val inlinks1 = inlinks.map(x=>if(x._2._1 == None)(x._1.toInt,(x._2._2._2.toFloat, x._2._2._1.toString()))else (x._1.toInt,(x._2._1.get.toFloat, x._2._2._1.toString())))
    val myrdduniq4 = myrdduniq3.join(inlinks1)
    val myrdduniq5 = myrdduniq4.map(x => x._2)
    val myrdduniq6 = myrdduniq5.join(mytitle)
    val myrdduniq7 = myrdduniq6.map(x => (x._2._1._1.toDouble, (x._2._1._2, x._2._2)))
    val myrdduniq8 = myrdduniq7.sortByKey(ascending=false).top(10)
    val myrdduniq9 = myrdduniq8.map(x => (x._2._2, x._2._1,x._1))
    //val myrdduniq10 = myrdduniq9.top(1)

    val distData = sc.parallelize(myrdduniq9)
    distData.saveAsTextFile(args(1));
    
    val b = a.map(value => (value._2,1))
    val d = b.reduceByKey((x, y) => x + y)
    val e = d.map(value => (value._1,(value._2.toFloat/c.toFloat)))
    
    e.saveAsTextFile(args(2));
    
    myrdd2.saveAsTextFile(args(3));
    //inlinks1.take(1000).foreach(println)
    //myrdduniq1.take(1000).foreach(println)
    //ranks.take(1000).foreach(println)
    sc.stop();
  }
}