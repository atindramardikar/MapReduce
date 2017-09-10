package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class PagerankMapper extends Mapper<Text, PageNode, Text, PageNode> {

    @Override
    public void map(Text pageID, PageNode node, Context ctx) throws IOException, InterruptedException {
        ctx.write(pageID, node); // emits the structure
        
        ArrayList<Text> outlinks = node.adjacencyList;
        
        // Compute contributions to send along outgoing links
        double p = node.pagerank / (double) outlinks.size();
        for (Text page : outlinks) {
            ctx.write(new Text(page), new PageNode(p, new ArrayList<Text>(), false));
        }
    }
}
