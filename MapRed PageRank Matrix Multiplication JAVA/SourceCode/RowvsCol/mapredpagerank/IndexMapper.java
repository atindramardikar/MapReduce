package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

//For the first map Reduce job while creating index we just calculate the number of input splits and the number of records per split
// For both the mapreduce job the mapper is common.
public class IndexMapper extends Mapper<Object, Text, Text, NullWritable> {
IntWritable partitionId;
long localrownum = 0;
ArrayList<String> outlinksArrayList; //to store the outlinks
    @Override
    public void setup(Context ctx){
       partitionId= new IntWritable(ctx.getConfiguration().getInt("mapred.task.partition", -1)); 
    }
    @Override
    public void map(Object _k, Text lines, Context ctx) throws IOException, InterruptedException {
        
        String line = lines.toString(); // read each line
        String[] tokens = line.split(":"); //split
        String pageId = tokens[0].trim(); //get pageID
        //Double pagerank = (double) 1 / totalNodes; //initital PageRank

        String[] outlinks = tokens[1].trim().split(",");
        outlinksArrayList = new ArrayList();

        //get the adjacency list (outlinks)
        for (String str : outlinks) {
            if (!"".equals(str.trim())) {
                outlinksArrayList.add(str.trim());
            }
        }
            ctx.write(new Text(pageId), NullWritable.get()); // emit the pageID
        for(String str:outlinksArrayList){
            ctx.write(new Text(str), NullWritable.get()); // emit every page from the adjacency list
        }
    }
}
