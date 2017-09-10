package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class PagerankReducer extends Reducer<LongWritable, PageNode, LongWritable, DoubleWritable> {

    // reduce recieves M[i][k] and R[k][j] for different i,j
    // we just do M[i][k]*R[k][j] which is the contribution for cell [i][j]
    @Override
    public void reduce(LongWritable rc, Iterable<PageNode> vals, Context ctx) throws IOException, InterruptedException {

        ArrayList<PageNode> M_list = new ArrayList();
        ArrayList<PageNode> R_list = new ArrayList();
        for (PageNode node : vals) {
            if ("M".equals(node.id.trim())) {
                M_list.add(new PageNode(node.val, node.rc, node.id));// add M[i][j] vals to M_list
            } else if ("R".equals(node.id.trim())) {
                R_list.add(new PageNode(node.val, node.rc, node.id));//add R[i][j] vals to R_lit
            }
        }
        for (PageNode nodeM : M_list) {
            for (PageNode nodeR : R_list) {
                ctx.write(new LongWritable(nodeM.rc), new DoubleWritable(nodeM.val * nodeR.val)); //multiply
            }
        }


    }
}
