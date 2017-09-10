package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InputMapper extends Mapper<Object, Text, Text, PageNode> {

    ArrayList<Text> outlinksArrayList;

    @Override
    public void map(Object _k, Text lines, Context ctx) throws InterruptedException, IOException {
        long totalNodes = ctx.getConfiguration().getLong("V", 0); //get the total number of nodes
        String line = lines.toString(); // read each line
        String[] tokens = line.split(":"); //split
        String pageId = tokens[0]; //get pageID
        Double pagerank = (double) 1 / totalNodes; //initital PageRank

        String[] outlinks = tokens[1].trim().split(",");
        outlinksArrayList = new ArrayList();

        //get the adjacency list (outlinks)
        for (String str : outlinks) {
            if (!"".equals(str.trim())) {
                outlinksArrayList.add(new Text(str.trim()));
            }
        }

        //emit the structure
        if (outlinksArrayList.isEmpty()) {
            // write pageID and Node. if no outlinks put danglingnode true
            ctx.write(new Text(pageId.trim()), new PageNode(pagerank, outlinksArrayList, true));
        } else {
            // write pageID and Node. if outlinks put danglingnode false
            ctx.write(new Text(pageId.trim()), new PageNode(pagerank, outlinksArrayList, false));
        }

        // Compute contributions to send along outgoing links
        double p = pagerank / (double) outlinksArrayList.size();

        for (Text node : outlinksArrayList) {
            ctx.write(node, new PageNode(p, new ArrayList<Text>(), false));
        }
    }
}
