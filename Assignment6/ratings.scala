import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object ratings{
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: ratings <input file>  <output file>")
        System.exit(1);
      }
    val conf = new SparkConf().setAppName("ratings app").setMaster("yarn")
    val sc = new SparkContext(conf)
    conf.set("spark.sql.warehouse.dir", "hdfs://bd-hmd05:9000/user/hive/warehouse")
    val spark = SparkSession.builder().master("yarn").getOrCreate()
    spark.conf.set("spark.sql.warehouse.dir", "hdfs://bd-hmd05:9000/user/hive/warehouse")
    //Creating a spark context
    
        
    val myrdd = sc.textFile(args(0))
    myrdd.foreach(println)
    val myrdd1 = myrdd.map(line=> line.split("\t"))
    val myrdd2 = myrdd1.map(line => (line(0),line(1)))
    val myrdd3 = myrdd2.flatMapValues(line => line.split("#"))

    val userAA = myrdd3.map(splits => (splits._1.toInt,splits._2.toInt))
    val userBB = myrdd3.map(splits => (splits._1.toInt,splits._2.toInt))
    
    val movie = sc.textFile(args(1))
    val movie1 = movie.map(line=> line.split("#"))
    val movie2 = movie1.map(line => (line(0).toInt,line(1)))
    System.out.println("marakhi1 :");
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val userADF3 = userAA.toDF()
    val userBDF3 = userBB.toDF()
    val movie3 = movie2.toDF()
    userADF3.createOrReplaceTempView("userAA")
    userBDF3.createOrReplaceTempView("userBB")
    movie3.createOrReplaceTempView("movie")
    
    val data = spark.sql("select a, c, count(*) as d, collect_set(e) from (select userAA._1 as a, userAA._2 as b, userBB._1 as c, userBB._2 as d, movie._2 as e from userAA,userBB,movie where userAA._2 = userBB._2 and userAA._1 < userBB._1 and userBB._2 = movie._1) tab3 where a != c group by a, c having d > 50 order by d desc")
    //val data = spark.sql("select a, c, count(*) as d, e from (select userAA._1 as a, userAA._2 as b, userBB._1 as c, userBB._2 as d, movie._2 as e from userAA,userBB,movie where userAA._2 = userBB._2 and userAA._1 < userBB._1 and userBB._2 = movie._1) tab3 where a != c group by a, c, e having d > 2")
    val data1 = data.rdd
    //System.out.println("marakhi :")
    //data1.foreach(println)
    //val data2 = data1.map(row=>(row.getAs[Int](0),row.getAs[Int](1),row.getAs[Int](2)))
    //val data3 = data2.map(value => if ( value._2 < value._1 )(value._2,value._1,value._3) else value).distinct
    //val data4 = data3.map(value => (value._1+ " " +value._2,value._3))
    
//    val data10 = spark.sql("select a, c, b from (select userAA._1 as a, userAA._2 as b, userBB._1 as c, userBB._2 as d from userAA,userBB where userAA._2 = userBB._2) tab3 where a != c")
//   //System.out.println("Count", data.count())
//    val data11 = data10.rdd
//    //data11.foreach(println)
//    //System.out.println("marakhi: ", data1.foreach(println))
//    val data12 = data11.map(row=>(row.getAs[Int](0),row.getAs[Int](1),row.getAs[Int](2)))
//    val data13 = data12.map(value => ifval data4 = data3.map(value => (value._1+ " " +value._2,value._3)) ( value._2 < value._1 )(value._2,value._1,value._3) else value).distinct
//    //System.out.println("marakhi: ", data2.foreach(println))
//    data13.foreach(println)
//    val data14 = data13.map(value => (value._1+ " " +value._2,value._3))
    
    data1.saveAsTextFile(args(2));
    sc.stop();
  }
} 