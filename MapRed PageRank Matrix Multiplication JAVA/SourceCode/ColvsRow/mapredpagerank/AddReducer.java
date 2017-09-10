package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

// We actually add the results in the reducer for each row. We also apply the actual PR formula and get the final PR for each page and push it to the R[i] matrix.
public class AddReducer extends Reducer<LongWritable, DoubleWritable, Text, NullWritable> {

    @Override
    public void reduce(LongWritable rc, Iterable<DoubleWritable> vals, Context ctx) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : vals) {
            sum += val.get(); //add up the sum
        }
        long dpr = ctx.getConfiguration().getLong("DanglingNodePR", -10);
        long V = ctx.getConfiguration().getLong("V", -10);
        
        //Calculate the Pagerank
        Double d = (double) dpr / 1000000000;
        sum += d;
        sum = sum * 0.85;
        sum += 0.15 / (double) V;
        //Push the new R[] to file
        ctx.write(new Text(rc.get() + ",0," + sum + ",R"), NullWritable.get());


    }
}
