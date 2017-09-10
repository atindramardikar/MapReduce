package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Class to keep track of the page-nodes of the link structure
class PageNode implements Writable {

    public double val; //stores the pagerank of the page
    public long rc;
    public String id;

    public PageNode() {
    }

    public PageNode(double val, long rc, String id) {
        this.val = val;
        this.rc = rc;
        this.id = id;
    }

    // abstract methods
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(val);
        out.writeUTF(id);
        out.writeLong(rc);
        
    }

    @Override
    public String toString() {
        return val + " " + id + " " + rc;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        val = in.readDouble();
        id = in.readUTF();
        rc = in.readLong();
    }
    
    @Override
    public boolean equals(Object c) {
        return false;
}

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.val) ^ (Double.doubleToLongBits(this.val) >>> 32));
        hash = (int) (47 * hash + this.rc);
        hash = 47 * hash + Objects.hashCode(this.id);
        return hash;
    }
}