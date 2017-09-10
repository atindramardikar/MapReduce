package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import mapredpagerank.MapRedPageRank.CountersPR;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Reducer;

// We actually add the results in the reducer for each row. Since we only have one row for dangling matrix DR will give us one value which we can use to calculate DR.
public class DanglingAddReducer extends Reducer <LongWritable, DoubleWritable, LongWritable, DoubleWritable> {


    @Override
    public void reduce(LongWritable rc, Iterable<DoubleWritable> vals, Context ctx) throws IOException, InterruptedException {
        double sum=0.0;
        for (DoubleWritable val : vals) {
            sum+=val.get();
        }
        ctx.write(rc, new DoubleWritable(sum));
        ctx.getCounter(CountersPR.danglingNodePR).increment((long) (sum * 1000000000)); //set the counter with calculated danglingPR.
        
    }
}
