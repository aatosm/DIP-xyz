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

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Flickr")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val raw     = lines.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(line => line.split(", ")).map(i => Photo(i(0).toString, i(1).toDouble, i(2).toDouble))
    
    val initialMeans = initializeMeans(kmeansKernels, raw)
    val means   = kmeans(initialMeans, raw)   
    
    println("alkup:")
    initialMeans.foreach { mean => 
      println(mean._1 + "," + mean._2)
    }
    
    println("lasketut:")
    means.foreach { mean => 
      println(mean._1 + "," + mean._2)
    }
    
    sc.stop()
  }
  
  
}


class Flickr extends Serializable {
  
/** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  def kmeansKernels = 8  
  
  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 50
  

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
   
  
  def initializeMeans(k: Int, photoRDD: RDD[Photo]): Array[(Double, Double)] = {   
    
    val means = photoRDD.takeSample(false, k, 23)
    means.map(m => (m.latitude, m.longitude)).toArray
  }
  
  def classify(means: Array[(Double, Double)], vectors: RDD[Photo]): RDD[((Double, Double), Iterable[Photo])] = {
    
    //val meanPhotoRdd = vectors.map(photo => (findClosest((photo.latitude, photo.longitude), means), photo))
    
    val grouppedRdd = vectors.groupBy(photo => means(findClosest((photo.latitude, photo.longitude), means)))    
    grouppedRdd
  }
  
  def averageVectors(ps: Iterable[Photo]): (Double, Double) = {
    var lat = 0.0
    var lon = 0.0
    
    ps.foreach { photo =>
      lat += photo.latitude
      lon += photo.longitude      
    }
    
    ((lat / ps.size), (lon / ps.size))
  }
  
  def update(classified: RDD[((Double, Double), Iterable[Photo])], oldMeans: Array[(Double, Double)]): Array[(Double, Double)] = {
    
    oldMeans.map(mean => averageVectors(classified.filter(g => g._1 == mean)
        .map(g => g._2)
        .first()))
    
  }
  
  def converged(oldMeans: Array[(Double, Double)], newMeans: Array[(Double, Double)]): Boolean = {
    (oldMeans zip newMeans).forall {
      case (oldMean, newMean) => distanceInMeters(oldMean, newMean) <= kmeansEta
    }
    
  }
    
  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
    val classified = classify(means, vectors)
    val newMeans = update(classified, means)
    
    if (!converged(means, newMeans) && iter < kmeansMaxIterations) {
      kmeans(newMeans, vectors, iter+1)
    } else {
      newMeans
    }
    
    
  }
    
}
