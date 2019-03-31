import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.SparkSession

object recommender{
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: recommender <input file>  <output file>")
        System.exit(1);
      }
    //Creating a spark context
    //val conf = new SparkConf().setAppName("graph app").setMaster("local[1]");
    //val sc = new SparkContext(conf)
    val conf = new SparkConf().setAppName("recommender app").setMaster("yarn")
    val sc = new SparkContext(conf)
    conf.set("spark.sql.warehouse.dir", "hdfs://bd-hmd05:9000/user/hive/warehouse")
    val spark = SparkSession.builder().master("yarn").getOrCreate()
    spark.conf.set("spark.sql.warehouse.dir", "hdfs://bd-hmd05:9000/user/hive/warehouse")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sc.setCheckpointDir("hdfs://bd-hmd05:9000/hadoop-user/data")
    val myrdd = sc.textFile(args(0))
    val myrdd1 = myrdd.map(line=> line.split(" "))
    val myrdd2 = myrdd1.map(line=> (line(1).toInt, (line(0).toInt, line(2).toDouble)))
    val myrdd3 = sc.textFile(args(1))
    val myrdd4 = myrdd3.map(line=> line.split("	"))
    val myrdd5 = myrdd4.map(line=> if (line.size > 1)(line(0).toInt, line(1).toInt)else (0,0)).filter(x => x != (0,0))
    val myrdd6 = myrdd2.leftOuterJoin(myrdd5)
    val myrdd7 = myrdd6.map(x=>if(x._2._2 == None)(x._2._1._1,x._1,x._2._1._2)else (x._2._1._1,x._2._2.get,x._2._1._2))
    val myrdd8 = myrdd7.map(line => Rating(line._1.toInt, line._2.toString().toInt, line._3.toDouble))
    //myrdd7.take(10).foreach(println)
    val Array(training, testing) = myrdd8.randomSplit(Array(0.8,0.2))
    val rank=1; val lambda = 0.01; val numIterations=20; val alpha=0.01;
    val model = ALS.trainImplicit(training, rank, numIterations, lambda, alpha)
    val testdata = testing.map { case Rating(user, artist, count) => (user, artist) }
    val predictions = model.predict(testdata).map { case Rating(user, artist, count) => (user, artist, count) }
    val table2 = predictions.toDF()
    table2.createOrReplaceTempView("predictions")
    //val data = spark.sql("select predictions._1, predictions._2, predictions._3, RANK() over (ORDER by predictions._3) AS rank from predictions group by ")
    val data = spark.sql("select predictions._1, predictions._2, predictions._3, RANK() over (Partition by predictions._1 Order by predictions._3, predictions._2, predictions._1) AS rank from predictions group by predictions._1, predictions._2, predictions._3 order by rank asc")
    val data1 = data.rdd
    val count = data1.map(row=>(row.getAs[Int](0),1))
    val count1 = count.reduceByKey(((x, y) => x+y))
    val data2 = data1.map(row=>(row.getAs[Int](0),(row.getAs[Int](1),row.getAs[Int](3).toFloat)))
    val datacount = data2.join(count1)
    val datacount1 = datacount.map(row => ((row._1, row._2._1._1),row._2._1._2.toFloat/row._2._2.toFloat))
    val data3 = testing.map { case Rating(user, artist, count) => ((user, artist), count.toFloat) }
    val data4 = data3.join(datacount1)
    val data5 = data4.map(x => (x._1._1,x._2._1))
    val data6 = data4.map(x => (x._1._1,x._2._1*x._2._2))
    val data7 = data5.reduceByKey((x, y) => x+y)
    val data8 = data6.reduceByKey((x, y) => x+y)
    val data9 = data8.join(data7)
    val data10 = data9.map(x => (x._1,x._2._1/x._2._2))
    val data11 = data10.count()
    val data12 = data10.map(x => x._2)
    //data4.take(100).foreach(println)
//    data1.take(100).foreach(println)
    val data13 = data12.reduce((x,y)=> x+y)
    val data14 = data13.toFloat/data11.toFloat
//    predictions.take(100).foreach(println)
//    data1.take(100).foreach(println)
//    data2.take(100).foreach(println)
//    data3.take(100).foreach(println)
//    data9.take(100).foreach(println)
//      myrdd5.take(100).foreach(println)
//      myrdd6.take(100).foreach(println)
//      myrdd7.take(100).foreach(println)
    System.out.println("Marakhi :", data14)
//    //data1.saveAsTextFile(args(1));
    sc.stop();
  }
}