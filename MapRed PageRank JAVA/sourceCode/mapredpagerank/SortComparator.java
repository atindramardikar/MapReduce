package mapredpagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author atindramardikar
 */

// KeyComparator is used to sort the data in descending order.
// By default the data from reducer is sorted in ascending order.
public class SortComparator extends WritableComparator {

    public SortComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DoubleWritable key1 = (DoubleWritable) wc1;
        DoubleWritable key2 = (DoubleWritable) wc2;
        return Double.compare(key2.get(), key1.get());
    }
}

