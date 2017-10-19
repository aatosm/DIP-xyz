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

import scala.util.Try

case class Photo(id: String,
                 latitude: Double,
                 longitude: Double,
                 datetime: Date)

                 
object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Flickr")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    /** Sample .csv files in array */
    val files = Array("src/main/resources/photos/dataForBasicSolution.csv",  // 0
                      "src/main/resources/photos/flickrDirtySimple.csv",     // 1
                      "src/main/resources/photos/flickr3D.csv",              // 2
                      "src/main/resources/photos/elbow.csv")                 // 3
    
    /** read .csv data into String-RDD, access file with array index */
    val lines = sc.textFile(files(1))
    
    /** filter out the first header-line */
    val linesWithoutHeader = lines.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
    
    val dateFormat = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss")
    
    /**  map String-RDD into Photo-RDD, only accepts lines with correct Date-format */
    val dirtyData = linesWithoutHeader.map(line => Try(line.split(", "))
                      .map(i => Photo(i(0).toString, i(1).toDouble, i(2).toDouble, dateFormat.parse(i(3)))))               
    val photoRdd = dirtyData.filter(_.isSuccess).map(_.get)                
    
    /** initialize k starting points randomly from the Photo-RDD */
    val initialMeans = initializeMeans(kmeansKernels, photoRdd)
    
    /** run k-means algorithm */
    val means = kmeans(initialMeans, photoRdd)   
    
    /** output prints */
    println("\nInitialized means (k=" + kmeansKernels + "):")
    initialMeans.foreach { mean => 
      println(mean._1 + ", " + mean._2)
    }
    
    println("-------------------------")
    
    println("K-means algorithm output:")
    means.foreach { mean => 
      println(mean._1 + ", " + mean._2)
    }
    
    println("-------------------------")
    
    sc.stop()
  }  
}


class Flickr extends Serializable {
  
/** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  def kmeansKernels = 16  
  
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
  
  
  /** initialize means with takeSample: Returns a fixed-size sampled subset of Photo-RDD in an array */
  def initializeMeans(k: Int, photoRDD: RDD[Photo]): Array[(Double, Double)] = {   
    
    val means = photoRDD.takeSample(false, k, 23) 
    means.map(m => (m.latitude, m.longitude)).toArray
  }
  
  
  /** Photos are groupped by a mean (center) which is closest to its coordinates */
  def classify(means: Array[(Double, Double)], vectors: RDD[Photo]): RDD[((Double, Double), Iterable[Photo])] = {
    
    val grouppedPhotoMeanRdd = vectors.groupBy(photo => means(findClosest((photo.latitude, photo.longitude), means)))    
    grouppedPhotoMeanRdd
  }
  
  
  /** Sums up total longitude and latitude of every Photo-object and averages them by size of Photo-objects
   *  --> creates a new mean (center) with averages */
  def averageVectors(ps: Iterable[Photo]): (Double, Double) = {
    
    var lat = 0.0
    var lon = 0.0
    
    ps.foreach { photo =>
      lat += photo.latitude
      lon += photo.longitude      
    }
    
    ((lat / ps.size), (lon / ps.size))
  }
  
  
  /** replaces old means (centers) with new, updated means which are solved with averageVectors() -function */
  def update(classified: RDD[((Double, Double), Iterable[Photo])], oldMeans: Array[(Double, Double)]): Array[(Double, Double)] = {
               
    oldMeans.map(matchingMean => averageVectors(classified.filter(group => group._1 == matchingMean)
        .map(group => group._2)
        .first()))
    
  }
  
  
  /** check if the convergence criteria fulfills = distance between every old-new mean pair is less than kmeansEta */
  def converged(oldMeans: Array[(Double, Double)], newMeans: Array[(Double, Double)]): Boolean = {
    
    (oldMeans zip newMeans).forall {
      case (oldMean, newMean) => distanceInMeters(oldMean, newMean) <= kmeansEta
    }    
  }
  
  
  /** tail recursive k-means algorithm */
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
