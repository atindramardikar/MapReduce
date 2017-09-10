package mapredpagerank;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author atindramardikar
 */
public class OutputMapper extends Mapper<Text, PageNode, DoubleWritable, Text> {

    @Override
    public void map(Text key, PageNode node, Mapper.Context ctx) throws InterruptedException, IOException {
        // Make rank as the key to facilitate the sorting.
        ctx.write(new DoubleWritable(node.pagerank), key);
    }
}
