package mapredpagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author atindramardikar
 */
public class SortReducer extends Reducer<NullWritable, Text, Text, DoubleWritable> {

    int count = 0; // to count top hundred
    Map<Long, String> PageToRow;
    private TreeMap<Double, Text> top100; //get overall top100

    //get the page to index mapping in setup with all the mapping inversed
    @Override
    public void setup(Context ctx) throws IOException {
        top100 = new TreeMap<Double, Text>();
        String path = ctx.getConfiguration().get("outputpath");
        Path awsPath = new Path(path + "/Finalindex");
        FileSystem fs = FileSystem.get(awsPath.toUri(), ctx.getConfiguration());
        FileStatus[] listStatus = fs.listStatus(awsPath);
        PageToRow = new HashMap<>(); //hashmap to store index mapping
        for (int i = 0; i < listStatus.length; i++) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(listStatus[i].getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                String pageId = tokens[0].trim();
                long index = Long.parseLong(tokens[1].trim());
                PageToRow.put(index, pageId);
            }
        }
    }

    // Use one reducer so that we can calculate overall top100
    @Override
    public void reduce(NullWritable dummy, Iterable<Text> pageIds, Context ctx) throws IOException, InterruptedException {
        // for all the pages print top 100
        for (Text value : pageIds) {
            String[] tokens=value.toString().split(",");
            top100.put(Double.parseDouble(tokens[1].trim()),new Text(PageToRow.get(Long.parseLong(tokens[0].trim()))));

            if (top100.size() > 100) {
                top100.remove(top100.firstKey());//put in global top 100
            }
        }

    
    
    for (double d : top100.descendingKeySet()) {
            ctx.write(top100.get(d),new DoubleWritable(d)); //write the final 100 to file.
        }
}
}

