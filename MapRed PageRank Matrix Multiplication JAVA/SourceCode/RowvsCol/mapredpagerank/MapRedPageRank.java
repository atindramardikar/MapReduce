
// Version2: Row by Column implementation of PageRank.

package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Driver class
public class MapRedPageRank {

    //Global counter to keep track of the Dangling PR Sum and the number of partitions created.
    static enum CountersPR {
        danglingNodePR,
        partitionCounter
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        // Calling all the functions
        Path parserOutput = new Path(args[1] + "/Parsedinput");
        parser(new Path(args[0]), parserOutput);
        long totalNodes = index(parserOutput, new Path(args[1] + "/index"), new Path(args[1] + "/Finalindex"));
        createMatrix(totalNodes, parserOutput, args[1]);
        for (int i = 1; i <= 10; i++) {
            long dpr = dangling(new Path(args[1] + "/matrices"), new Path(args[1] + "/iter" + (i - 1)), new Path(args[1] + "/intermediatedpr" + i), new Path(args[1] + "/dR" + i),0);
        
        multi(totalNodes, new Path(args[1] + "/matrices"), new Path(args[1] + "/iter" + (i - 1)), new Path(args[1] + "/intermediateIter" + i), new Path(args[1] + "/iter" + i), dpr);

        }
        writeOutput(conf, new Path(args[1] + "/iter10"), args[1]);
    }

    //parser parses the input and convert to a Node: adjacency List format
    public static void parser(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "parser");
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(Parser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }

    // Two jobs to assign the index for the pages
    // First job to calculate number of partitions and number of records per partition.
    // Second job to assign actual index to the pages
    public static long index(Path input, Path output, Path output1) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "index-calculate partitions");
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

        long partition = job.getCounters().findCounter(CountersPR.partitionCounter).getValue();
        Job job2 = Job.getInstance(conf, "assign index");
        long sum = 0;
        job2.getConfiguration().setLong("partition0", 0);
        //In this block of code I assign the offset values for each input split.
        // So for the partition) its 0, for 1 its 0+partition0 and so on.
        for (int i = 1; i < partition; i++) {
            sum += job.getCounters().findCounter("partition" + (i - 1), "p").getValue();
            job2.getConfiguration().setLong("partition" + i, sum);
        }
        long totalNodes = 0;
        for (int i = 0; i < partition; i++) {
            totalNodes += job.getCounters().findCounter("partition" + i, "p").getValue();
        }
        job2.setJarByClass(MapRedPageRank.class);
        job2.setMapperClass(IndexMapper.class);
        job2.setReducerClass(FinalIndexReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, input);
        FileOutputFormat.setOutputPath(job2, output1);
        job2.waitForCompletion(true);

        return totalNodes;
    }

    
    // Creation of M,D,R[0] matrices
    private static long createMatrix(long totalNoOfNodes, Path input, String output) throws IOException, InterruptedException, ClassNotFoundException, Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "create matrix");
        job.getConfiguration().setLong("V", totalNoOfNodes);
        job.getConfiguration().set("outputpath", output);

        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output + "/matrices"));
        MultipleOutputs.addNamedOutput(job, "R", TextOutputFormat.class,
                Text.class, NullWritable.class);
        boolean ok = job.waitForCompletion(true);
        // get the DanglingPRSum calculated in the iteration
        long newDanglingPR = job.getCounters().findCounter(CountersPR.danglingNodePR).getValue();
        if (!ok) {
            throw new Exception("Job failed");
        }
        return newDanglingPR; //return the DanglingPRSum
    }

    //one MapRed job to do complete matrix multiplication.
    private static long dangling(Path input, Path input1, Path output, Path finaloutput,int i) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate Dangling- Multiply DR");
        job.getConfiguration().set("outputpath", input1.toString());
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(DanglingMapper.class);
        //job.setNumReduceTasks(0);
        job.setReducerClass(DanglingReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PageNode.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, finaloutput);
        boolean ok = job.waitForCompletion(true);

        if (!ok) {
            throw new Exception("Job1 failed");
        }
        long newDanglingPR = job.getCounters().findCounter(CountersPR.danglingNodePR).getValue();
        return newDanglingPR;
    }

    //one MapRed job to do complete matrix multiplication.
    private static void multi(long totalNodes, Path input, Path input1, Path output, Path finaloutput, long dpr) throws IOException, InterruptedException, ClassNotFoundException, Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "multiplay MR");
        job.getConfiguration().set("outputpath", input1.toString());
        job.getConfiguration().setLong("V", totalNodes);
        job.getConfiguration().setLong("DanglingNodePR", dpr);
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(PagerankMapper.class);
        job.setReducerClass(PagerankReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PageNode.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, finaloutput);
        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job1 failed");
        }
    }

    // WriteOutput top100 Pagerank pages
    public static void writeOutput(Configuration conf, Path input, String output) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        Job job = Job.getInstance(conf, "write output");
        job.getConfiguration().set("outputpath", output);
        job.setJarByClass(MapRedPageRank.class);
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output + "/top100"));

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

    }
}
