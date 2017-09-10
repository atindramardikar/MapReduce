package mapredpagerank;
/**
 *
 * @author atindramardikar
 */
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

// Driver class
public class MapRedPageRank {

    //Global counter to keep track of the Dangling PR Sum
    static enum CountersPR {
        danglingNodePR
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Path parserOutput=new Path(args[1]+"/Parsedinput");
        long totalNoOfNodes = parser(new Path(args[0]),parserOutput); //parser return the totalNoofnodes 
        
        //initial iteration return the DanglingnodePR for next iteration
        long newDanglingPr = readInputandIterate(totalNoOfNodes, 0,parserOutput,new Path(args[1]+"/iter0"));
        long newSum;
        int ii = 0;
        
        //iterate 10 times after first
        while (ii < 10) {
            //after every iteration get the DanglingSumPR and pass for next iteration
            newSum = iterate(ii, totalNoOfNodes, newDanglingPr,new Path(args[1]+"/iter"+ii),new Path(args[1]+"/iter"+(ii+1)));
            newDanglingPr = newSum;
            ii++;
        }
        writeOutput(conf,new Path(args[1]+"/iter10"),new Path(args[1]+"/FinalTop100"));
    }

    //parser parses the input and convert to a Node: adjacency List format
    public static long parser(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "parser");
        conf.setInt("numberOfNodes", 0);
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(Parser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(conf);
        //fs.delete(input, true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean ok = job.waitForCompletion(true);
        long count = job.getCounters().findCounter("number", "numberofNodes").getValue();

        if (!ok) {
            throw new Exception("Job failed");
        }
        return count; //return the total number of pages
    }

    private static long readInputandIterate(long totalNoOfNodes, long danglingNodePR, Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "read input");
        //set variables to be used to calculate new PageRank
        job.getConfiguration().setLong("DanglingNodePR", danglingNodePR);
        job.getConfiguration().setLong("V", totalNoOfNodes);
        job.getConfiguration().setLong("VR", totalNoOfNodes);
        conf.setInt("new", 0);
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(InputMapper.class);
        job.setReducerClass(PagerankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageNode.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean ok = job.waitForCompletion(true);
        // get the DanglingPRSum calculated in the iteration
        long newDanglingPR = job.getCounters().findCounter(CountersPR.danglingNodePR).getValue();
        if (!ok) {
            throw new Exception("Job failed");
        }
        return newDanglingPR; //return the DanglingPRSum

    }

    public static long iterate(int ii, long totalNoOfNodes, long danglingNodePR, Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "Pagerank iteration");
        //set variables to be used to calculate new PageRank
        job.getConfiguration().setLong("DanglingNodePR", danglingNodePR);
        job.getConfiguration().setLong("VR", totalNoOfNodes);
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(PagerankMapper.class);
        job.setReducerClass(PagerankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageNode.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);
        // get the DanglingPRSum calculated in the iteration
        long danglingnode = job.getCounters().findCounter(CountersPR.danglingNodePR).getValue();
        if (!ok) {
            throw new Exception("Job failed");
        }
        return danglingnode; //return the DanglingPRSum for next iteration

    }

    // WriteOutput top100 Pagerank pages
    public static void writeOutput(Configuration conf,Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        Job job = Job.getInstance(conf, "write output");
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);//everything to one reducer to calculate top100
        job.setSortComparatorClass(SortComparator.class); //descending order
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

    }
}
