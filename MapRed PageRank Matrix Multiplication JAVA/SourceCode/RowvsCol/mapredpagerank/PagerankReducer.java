package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import mapredpagerank.MapRedPageRank.CountersPR;

import org.apache.hadoop.mapreduce.Reducer;

public class PagerankReducer extends Reducer<LongWritable, PageNode, Text, NullWritable> {

    Map<Long, Double> R;
    
    //We get the complete R in the setup.
    @Override
    public void setup(Context ctx) throws IOException, InterruptedException {
        String path = ctx.getConfiguration().get("outputpath");
        Path awsPath = new Path(path);
        FileSystem fs = FileSystem.get(awsPath.toUri(), ctx.getConfiguration());
        FileStatus[] listStatus = fs.listStatus(awsPath);
        R = new HashMap<>();
        for (int i = 0; i < listStatus.length; i++) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(listStatus[i].getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                long index = Long.parseLong(tokens[0].trim());;
                double rank = Double.parseDouble(tokens[2].trim());
                R.put(index, rank);
            }
        }
    }


    @Override
    public void reduce(LongWritable rc, Iterable<PageNode> vals, Context ctx) throws IOException, InterruptedException {
        double sum=0;
        for (PageNode node : vals) {
            sum+=(node.val*R.get(node.rc)); //multiply
        }
        // Calculate pagerank and emit
        long dpr = ctx.getConfiguration().getLong("DanglingNodePR", -10);
        long V = ctx.getConfiguration().getLong("V", -10);
        Double d = (double) dpr / 1000000000;
        sum += d;
        sum = sum * 0.85;
        sum += 0.15 / (double) V;
        ctx.write(new Text(rc.get() + ",0," + sum + ",R"), NullWritable.get());

    }
}
