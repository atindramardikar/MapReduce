package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

//Here we actually assign the index to page. We get the offset and the index is just offset+ (counter++)
public class FinalIndexReducer extends Reducer <Text, NullWritable, Text, LongWritable> {

    long count;
    int partitionID;
    long offset;
    
    
    @Override
    public void setup(Context ctx){
        count=0;
        partitionID=ctx.getConfiguration().getInt("mapred.task.partition", -1); //get the partitionID
        offset=ctx.getConfiguration().getLong("partition"+partitionID, 0);// get the offset
    }

    @Override
    public void reduce(Text pageId, Iterable<NullWritable> vals, Context ctx) throws IOException, InterruptedException {
       
        ctx.write(pageId, new LongWritable(offset+count)); //add offset to the counter and emit.
         count++;
    }
}
