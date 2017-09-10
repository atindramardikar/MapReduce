package mapredpagerank;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author atindramardikar
 */
public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
int count = 0; // to count top hundred
    public void reduce(DoubleWritable Rank, Iterable<Text> pageId, Context ctx) throws IOException, InterruptedException {
        // for all the pages print top 100
        for (Text page : pageId) {
            if (count < 100) {
                ctx.write(page, Rank); // writing to final output
                count++;
            }
        }
    }
}
