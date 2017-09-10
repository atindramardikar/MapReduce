
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

object Prediction {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
		val sc = new SparkContext(conf)

		val models= new Array[org.apache.spark.mllib.classification.LogisticRegressionModel](10)

		// get all the models
		for(i <- 0 to 9){
  				models(i)= LogisticRegressionModel.load(sc, args(0)+"/scalaLogisticRegressionWithLBFGSModel"+i)
		}
		
		// Read the unlabeled file
		val parseUnlabeled= sc.textFile(args(0)+"/unlabeled.csv.bz2")
		
		//PreProcessing of the data
		val paramUnlabeled= parseUnlabeled
	 				.map(line=> line.replaceAll("\\?","0")) //replace ? with 0
					.map(line=> line.replaceAll("X","1")) //replace X with 1
					.map (line=>line.split(","))
					.filter(fields => fields(26)!="Agelaius_phoeniceus")
					//selecting the columns needed for the model and a suitable label(bird)
					.map{fields => 
						(fields(0),LabeledPoint(0.0,Vectors.dense(fields(2).toDouble,
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


		val predictionAndLabels1 = paramUnlabeled.map { case (samplingID, LabeledPoint(label,features)) =>
			var positive=0;
			for(i<- 0 to 9){
				val prediction = models(i).predict(features) //prediction
				if(prediction==1.toDouble)
				 positive=positive+1 //Calculate positives
			}
			if((positive.toDouble/10.toDouble).toDouble < 0.5) //Calculate average and make final prediction
				samplingID+",0.0"
			else
				samplingID+",1.0"	
		}

		predictionAndLabels1.saveAsTextFile(args(0)+"/Prediction")

		sc.stop()
	}
}