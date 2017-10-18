package assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

import java.io.StringReader
import com.opencsv.CSVReader

import java.util.Date
import java.text.SimpleDateFormat
import Math._

import scala.util.Random
import scala.collection.mutable.ArrayBuffer


case class Photo(id: String,
                 latitude: Double,
                 longitude: Double)
                 //datetime: Date)

                 
object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val raw     = lines.flatMap(line => line.split(", ")).map(i => Photo(i(0).toString, i(1).toDouble, i(2).toDouble))
    
    val initialMeans = initializeMeans(raw)
    val means   = kmeans(initialMeans, raw)   
    
  }
}


class Flickr extends Serializable {
  
/** K-means parameter: Convergence criteria */
  val kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  val kmeansKernels = 16  
  
  /** K-means parameter: Maximum iterations */
  val kmeansMaxIterations = 50
  
  var kmeansCurrentIteration = 0

  //(lat, lon)
  def distanceInMeters(c1: (Double, Double), c2: (Double, Double)) = {
		val R = 6371e3
		val lat1 = toRadians(c1._1)
		val lon1 = toRadians(c1._2)
		val lat2 = toRadians(c2._1)
		val lon2 = toRadians(c2._2)
		val x = (lon2-lon1) * Math.cos((lat1+lat2)/2);
		val y = (lat2-lat1);
		Math.sqrt(x*x + y*y) * R;
  }
  
    
  /** Return the index of the closest mean */
  def findClosest(p: (Double, Double), centers: Array[(Double, Double)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = distanceInMeters(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }
  
  def initializeMeans(photoRDD: RDD[Photo]): Array[(Double, Double)] = {
    val means = Array((60.5D, 22.0D), (61.5D, 24.0D))
    means
  }
  
  def classify(means: Array[(Double, Double)], vectors: RDD[Photo]): RDD[(Int, Photo)] = {
    val meanPhotoRdd = vectors.map(photo => (findClosest((photo.latitude, photo.longitude), means), photo))
    meanPhotoRdd
  }
  
  def update(classified: RDD[(Int, Photo)], oldMeans: Array[(Double, Double)]): Array[(Double, Double)] = {
    var ab = ArrayBuffer[(Double, Double)]()
    val newRdd = classified.map(tuple => (tuple._1, (tuple._2.latitude, tuple._2.longitude)))
    for (i <- 0 to oldMeans.length) {
       val pointsMappedToCenter = newRdd.filter(tuple => tuple._1 == i)
       val pointsCount = pointsMappedToCenter.count
       val sumOfCurrentPoints = pointsMappedToCenter.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).first
       val averageOfPoints = ((sumOfCurrentPoints._2._1 / pointsCount), (sumOfCurrentPoints._2._2 / pointsCount))
       ab += averageOfPoints
    }
    val arrayOfNewMeans = ab.toArray
    arrayOfNewMeans
    
  }
  
  def converged(oldMeans: Array[(Double, Double)], newMeans: Array[(Double, Double)]): Boolean = {
    
  }
    
  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo]): Array[(Double, Double)] = {
    val classified = classify(means, vectors)
    val newMeans = update(classified, means)
    
    if (!converged(means, newMeans)) {
      kmeansCurrentIteration += 1
      kmeans(newMeans, vectors)
    } else {
      newMeans
    }
    
    
  }
    
}
