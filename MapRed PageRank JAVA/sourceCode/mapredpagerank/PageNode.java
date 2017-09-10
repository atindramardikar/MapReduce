package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Class to keep track of the page-nodes of the link structure
class PageNode implements Writable {

    public double pagerank; //stores the pagerank of the page
    public ArrayList<Text> adjacencyList; // stores the outlinks from that page
    public boolean danglingNode; // true if node is a dangling node

    public PageNode() {
    }

    public PageNode(double pagerank, ArrayList<Text> adjacencyList, boolean danglingNode) {
        this.pagerank = pagerank;
        this.adjacencyList = adjacencyList;
        this.danglingNode = danglingNode;
    }

    // abstract methods
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(pagerank);
        out.writeBoolean(danglingNode);
        out.writeInt(adjacencyList.size());
        for (Text node : adjacencyList) {
            node.write(out);

        }
    }

    @Override
    public String toString() {
        return pagerank + " " + adjacencyList + " " + danglingNode;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pagerank = in.readDouble();
        danglingNode = in.readBoolean();
        int nn = in.readInt();
        adjacencyList = new ArrayList<Text>(nn);
        for (int ii = 0; ii < nn; ii++) {
            Text t = new Text();
            t.readFields(in);
            adjacencyList.add(t);

        }
    }
}