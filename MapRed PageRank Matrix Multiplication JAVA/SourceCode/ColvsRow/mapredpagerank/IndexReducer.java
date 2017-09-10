package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import mapredpagerank.MapRedPageRank.CountersPR;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

// We calculate the number of input split and the number of records in each input split.
public class IndexReducer extends Reducer<Text, NullWritable, IntWritable, LongWritable> {

    long maxVal;

    @Override
    public void setup(Context ctx) {
        maxVal = 0;
    }
    
    @Override
    public void reduce(Text pageId, Iterable<NullWritable> vals, Context ctx) throws IOException, InterruptedException {
        maxVal++; //calculate the total number of records in the input split.
    }

    @Override
    public void cleanup(Context ctx) throws IOException, InterruptedException {
        IntWritable partitionId = new IntWritable(ctx.getConfiguration().getInt("mapred.task.partition", -1));
        ctx.getCounter(CountersPR.partitionCounter).increment(1); //increment the counter for number of input spllit
        ctx.getCounter("partition" + ctx.getConfiguration().getInt("mapred.task.partition", -1), "p").increment(maxVal); //increment the counter for number of records in this split
        ctx.write(partitionId, new LongWritable(maxVal));
    }
}
