
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
import mapredpagerank.Parser;

//references: stackoverflow.com
//references: http://spark.apache.org/docs/latest/

object PageRank {
    def main(args: Array[String]) {
        val conf = new SparkConf().
            setAppName(" Top 100 ")
            .setMaster("local")

        val sc = new SparkContext(conf)

        

        //parse the input bz2 files and map them with the java parser
		val parse=sc.textFile(args(0)) //read the input file
							.map(line => new Parser().pageRank(line.toString)) //call java function
							.filter(line=> !line.equals(""))//filter out all the blank lines 

		

		// split the output of parser and map in the form (pageID,outlinks)
		val outLinks= parse
						.map(line=> line.split(":")) //split
						.map(fields => if(fields.length>1)(fields(0).trim(),fields(1).trim().split(";").toList) //if there are outlinks put as a List
										else (fields(0).trim(), List())) //else put empty list
										.cache() // cache this for better performance as we will be using this structure for every iteration.

		

		val totalNoOfNodes = outLinks.count //get the total number of nodes

		val initialPR= 1.toDouble/totalNoOfNodes.toDouble // set initial PR

		var pageRank=outLinks.mapValues(node => initialPR) // set the initital PR for every pageID
		
		var DanglingPR= 0.toDouble; //danglingPR for first step is 0

		
		//run 10 iterations
		for(i <- 1 to 10){

			val join= outLinks.join(pageRank) //join outlinks and pagerank
							.map(page => (page._1, page._2._1, page._2._2)) //flatten the join

			//calculate the danglingPR sum which will be used for the next iteration
			var newdanglingNodesPR = join
								.filter(lines=>lines._2.size<1)
								.map(lines=> lines._3) // assign PR
								.reduce((sum, temp) => sum + temp) //add the PR


			// Calculate the contributions from the outlinks					
			val contriFromOutlink= join
							.filter(lines=>lines._2.size>0)
							.flatMap{ case (page,links, pageR) =>links.map(link => (link, pageR / links.size))}
						

			// update the pageranks using the formula
			pageRank= contriFromOutlink.reduceByKey((sum, temp) => sum + temp)
							.mapValues(node => (0.15/totalNoOfNodes.toDouble + 0.85 * (node + (DanglingPR/totalNoOfNodes.toDouble))))

			//update the dangling PR with the newly calculated one
			DanglingPR = newdanglingNodesPR
    }


    val sortedList=pageRank.map(_.swap) //swap key and value
					.sortByKey(false) //sort in descending order
					.map(_.swap) //swap back


	val firstHundred=sortedList.take(100) //take top100

	val finalSorted=sc.parallelize(firstHundred)// convert to RDD

	finalSorted.saveAsTextFile(args(1)+"/finalTop100") //write to file

	sc.stop()
	}
}