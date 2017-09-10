package mapredpagerank;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author atindramardikar
 */
public class OutputMapper extends Mapper<Object, Text, NullWritable, Text> {

    Map<String, Long> PageToRow;
    private TreeMap<Double, Long> top100; //TreeMap to store top 100 for the input split
    
    //for every input split send top 100
    @Override
    public void setup(Context ctx) throws IOException {
        top100 = new TreeMap<Double, Long>();
    }

    @Override
    public void map(Object _k, Text lines, Context ctx) throws InterruptedException, IOException {
        String line=lines.toString();
        String[]tokens=line.split(",");
        long pageId=Long.parseLong(tokens[0].trim()); //get the pageIndex
        double rank=Double.parseDouble(tokens[2].trim()); //get the rank
        top100.put(rank, pageId);

        if (top100.size() > 100) {
            top100.remove(top100.firstKey()); //keep first 100
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (double d : top100.keySet()) {
            context.write(NullWritable.get(),new Text(top100.get(d)+","+d)); //write top-100 for the input split
        }
    }
}
