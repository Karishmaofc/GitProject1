import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object customerorder extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
    
  val sc = new SparkContext("local[*]","customerorder")
  
  val input = sc.textFile("C:/Users/cwagh/OneDrive/Desktop/BigDataDatasets/Week9/customerorders-201008-180523.csv")
  
  val refinedIP = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))  
      
  val finalOP = refinedIP.reduceByKey((x,y) => x+y)
  
  val sortedOP = finalOP.sortBy(x => x._2)
      
  sortedOP.collect().foreach(println)
  
}