
import collection.immutable.BitSet

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils

// References: 
// www.stackoverflow.com
// http://spark.apache.org/docs/latest/
// https://spark.apache.org/docs/latest/mllib-linear-methods.html


object Holder extends Serializable {
 
  def ds(conf: SparkConf) = {
    //val conf = new SparkConf().setAppName("Simple")..set("spark.driver.allowMultipleContexts", "true").setMaster("local")
	@transient val sc = new SparkContext(conf)
	println("here")
	sc.stop()
  }
 }

object CreateModel extends Serializable {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
		@transient val sc = new SparkContext(conf)
		//val rdd = sc.newAPIHadoopRDD()


		// Read the input file
		val parseLabeled= sc.textFile("labeled.csv.bz2")

		//PreProcessing of the data
		val param= parseLabeled
		.map(line=> line.replaceAll("\\?","0")) //replace ? with 0
		.map(line=> line.replaceAll("X","1")) //replace X with 1
		.map (line=>line.split(","))
		.filter(fields => fields(26)!="Agelaius_phoeniceus")
		//selecting the columns needed for the model and a suitable label(bird)
		.map{fields => if(fields(26)=="X" || fields(26).toInt==0) 
			(LabeledPoint(0.0,Vectors.dense(fields(2).toDouble,
								    	    fields(3).toDouble,
								    	    fields(4).toDouble,
								    	    fields(5).toDouble,
								    	    fields(6).toDouble,
								    	    fields(7).toDouble,
								    	    fields(960).toDouble,
								    	    fields(963).toDouble,
								    	    fields(966).toDouble,
								    	    fields(967).toDouble,
								    	    fields(956).toDouble,
								    	    fields(955).toDouble,
								    	    fields(957).toDouble,
								    	    fields(958).toDouble,
								    	    fields(837).toDouble,
								    	    fields(493).toDouble,
								    	    fields(959).toDouble)))
			else
			(LabeledPoint(1.0,Vectors.dense(fields(2).toDouble,
								    	    fields(3).toDouble,
								    	    fields(4).toDouble,
								    	    fields(5).toDouble,
								    	    fields(6).toDouble,
								    	    fields(7).toDouble,
								    	    fields(960).toDouble,
								    	    fields(963).toDouble,
								    	    fields(966).toDouble,
								    	    fields(967).toDouble,
								    	    fields(956).toDouble,
								    	    fields(955).toDouble,
								    	    fields(957).toDouble,
								    	    fields(958).toDouble,
								    	    fields(837).toDouble,
								    	    fields(493).toDouble,
								    	    fields(959).toDouble)))
		}

		// Split data as 70:30 for training and testing
		val splits = param.randomSplit(Array(0.7, 0.3), seed = 11L)
		val training = splits(0).cache()
		val test = splits(1)

		//Divide the data into K partitions
		val trainingData=training.map { case LabeledPoint(label, features) =>
  				val rand=scala.util.Random.nextInt(10)
  				(rand,LabeledPoint(label,features))
  				}
  				.groupByKey() //Group all records for each partition
  				
  				val ss= trainingData.mapValues(valuess=> Holder.ds(conf))
  				ss.take(10).foreach(println)

  		val temp2= trainingData.collect()


		val models= new Array[org.apache.spark.mllib.classification.LogisticRegressionModel](10)

		for(val1 <- temp2){
				//create K models for K partition
				val model = new LogisticRegressionWithLBFGS()
  									.setNumClasses(2)
  									.run(sc.parallelize(val1._2.toSeq))
  				models(val1._1)= model
  				model.save(sc, "scalaLogisticRegressionWithLBFGSModel"+val1._1) //save models to file
		}
		
		//Compute raw scores on the test set.
		val predictionAndLabels = param.map { case LabeledPoint(label,features) =>
			var positive=0;
			for(i<- 0 to 9){
				val prediction = models(i).predict(features)
				if(prediction==1.toDouble)
				 positive=positive+1 //Calculate positives
			}
			if((positive.toDouble/10.toDouble).toDouble >0.5) //calculate average and make final prediction
				(label,1.0)
			else
				(label,0.0)	
		}

		// Metrics to check accuracy for test data
		val metrics = new MulticlassMetrics(predictionAndLabels)
		val accuracy = metrics.accuracy
		println("Accuracy ="+accuracy)


		sc.stop()
	}
}
