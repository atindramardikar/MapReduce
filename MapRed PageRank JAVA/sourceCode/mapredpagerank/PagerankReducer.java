package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import java.util.ArrayList;
import mapredpagerank.MapRedPageRank.CountersPR;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class PagerankReducer extends Reducer<Text, PageNode, Text, PageNode> {

    double s;
    String page;
    ArrayList<Text> tempAdjacencyList;

    @Override
    public void reduce(Text pageID, Iterable<PageNode> vals, Context ctx) throws IOException, InterruptedException {
        s = 0.0; //initialize the sum to zero for each reduce call
        page = pageID.toString();
        
        // Use temp variables instead of assigning complete node to ensure no Null pointer exception is thrown
        tempAdjacencyList = new ArrayList<>(); //temp adjacency list
        boolean danglingflag = false; // a temp flag to see if the node is dangling

        for (PageNode node : vals) {  
            if (node.danglingNode) {
                //if dangling node, increment the global counter for the danglingPR sum
                // this PR sum is used in the next iteration
                // also set the temp variables
                ctx.getCounter(CountersPR.danglingNodePR).increment((long) (node.pagerank * 10000000));
                tempAdjacencyList = node.adjacencyList;
                danglingflag = true;
            } else if (!node.adjacencyList.isEmpty()) {
                // if not a dangling just update the adjacency list
                tempAdjacencyList = node.adjacencyList;
            } else if (node.adjacencyList.isEmpty() && node.danglingNode == false) {
                // else just update the sum
                s += node.pagerank;
            }
        }

        // get total no of nodes and previous dangling node PR sum to calculate new pagerank
        long V = ctx.getConfiguration().getLong("VR", -10);
        long dpr = ctx.getConfiguration().getLong("DanglingNodePR", -10);
        
        //update the pagerank
        double pagerank = (0.15 / V) + ((0.85 * s) + (0.85 * (dpr / (V * 10000000))));
        
        //emit the page and Node with the updated value for the next iteration.
        ctx.write(new Text(page), new PageNode(pagerank, tempAdjacencyList, danglingflag));
    }
}
