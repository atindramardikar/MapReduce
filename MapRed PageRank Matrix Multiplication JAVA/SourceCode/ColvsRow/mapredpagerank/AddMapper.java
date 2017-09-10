package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

// We add the multiplication row-wise
// For mapper we just read the intermediate result and forward it as it is.
public class AddMapper extends Mapper<Object, Text, LongWritable, DoubleWritable> {

    @Override
    public void map(Object _k, Text lines, Context ctx) throws IOException, InterruptedException {
        String line = lines.toString();
        String[] tokens = line.split("\t");
        long col = Long.parseLong(tokens[0].trim());
        double val = Double.parseDouble(tokens[1].trim());
        ctx.write(new LongWritable(col), new DoubleWritable(val));
  //      ctx.write(_k, lines);
    }
}
