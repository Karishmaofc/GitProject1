import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object wordcount extends App
{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  // def main(args: Array[String]) = {
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/cwagh/OneDrive/Desktop/BigDataDatasets/Week9/search_data-201008-180523.txt")
  
  val words = input.flatMap(x => x.split(" "))
  
  val wordsLower = words.map(x => x.toLowerCase())
  
  val wordMap = wordsLower.map(x => (x,1))
  
  val finalCount = wordMap.reduceByKey((x,y) => x+y)
  
  val reverseTuple = finalCount.map(x => (x._2,x._1))
  
  val sortedResults = reverseTuple.sortByKey(false)
  
  val rev = sortedResults.map(x => (x._1, x._2))
  
  rev.collect.foreach(println)
  
    

scala.io.StdIn.readLine()
  

}