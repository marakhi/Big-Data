import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object symmetric{
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: symmetric <input file>  <output file>")
        System.exit(1);
      }
    //Creating a spark context
    val conf = new SparkConf().setAppName("symmetric app").setMaster("yarn");
    val sc = new SparkContext(conf)
    
    val myrdd = sc.textFile(args(0))
    val myrdd1 = myrdd.map(line=> line.split(" "))
    val userA = myrdd1.map(splits => (splits(0).toInt,splits(1).toInt))
    val userB = myrdd1.map(splits => (splits(1).toInt,splits(0).toInt))
    val inter = userA.intersection(userB)
    val inter1 = inter.map(value => if (value._2 < value._1)value._2+" "+value._1 else (value._1+" "+value._2)).distinct()
    
    inter1.saveAsTextFile(args(1));
    sc.stop();
  }
}