package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

// We do MR where M is VXV and R is VX1 so we will get VX1 which will be R for next iteration.
public class PagerankMapper extends Mapper<Object, Text, LongWritable, PageNode> {

    @Override
    public void map(Object _k, Text lines, Context ctx) throws IOException, InterruptedException {
        String line = lines.toString();
        String[] tokens = line.split(",");
        long row = Long.parseLong(tokens[0]);
        long col = Long.parseLong(tokens[1]);
        double val = Double.parseDouble(tokens[2]);
        String id = tokens[3];
        if ("M".equals(id.trim())) {
            ctx.write(new LongWritable(col), new PageNode(val, row, id));//since its colbyRow; For first Matrix we emit colNumber;
        } else if ("R".equals(id.trim())) {
            ctx.write(new LongWritable(row), new PageNode(val, col, id));// For second Matrix we emit rowNumber;
        }
    }
}
