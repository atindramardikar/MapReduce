package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// I store M and D in the same location as it is used in every iteration
// R[0] is stored in the iter) folder in this job itself
// I store the M and D matrix as sparse matrix in the format (row,col,val,MatrixId)
public class MatrixReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    ArrayList<String> outlinksArrayList; //To store the outlinks for each page
    Map<String, Long> PageToRow; // To store the page to index mapping
    private MultipleOutputs mos; //Store the R[0] in different location at iter0
    HashSet<Long> remaining; //to handle the dead nodes
    double pagerank;
    
    //get the page to index mapping in setup
    @Override
    public void setup(Context ctx) throws IOException, InterruptedException {
        remaining=new HashSet<>();
        String path = ctx.getConfiguration().get("outputpath");
        Path awsPath = new Path(path + "/Finalindex");
        FileSystem fs = FileSystem.get(awsPath.toUri(), ctx.getConfiguration());
        FileStatus[] listStatus = fs.listStatus(awsPath);
        PageToRow = new HashMap<>();
        for (int i = 0; i < listStatus.length; i++) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(listStatus[i].getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                String pageId = tokens[0].trim();
                long index = Long.parseLong(tokens[1].trim());
                PageToRow.put(pageId, index); //add to the mapping data-structure
                remaining.add(index); //add page index to the structure to handle dead nodes
            }
        }
        System.out.println(remaining.size());
        mos = new MultipleOutputs(ctx);
        pagerank = (double) 1 / PageToRow.size(); //initital PageRank
        for(int i=0;i<PageToRow.size();i++){
            mos.write("R", new Text(i + ",0," + pagerank + ",R"), NullWritable.get(), path + "/iter0/r");
        }
    }

    @Override
    public void reduce(Text lines, Iterable<NullWritable> vals, Context ctx) throws IOException, InterruptedException {
        
        long totalNodes = ctx.getConfiguration().getLong("V", 0); //get the total number of nodes
        String path = ctx.getConfiguration().get("outputpath");
        String line = lines.toString(); // read each line
        String[] tokens = line.split(":"); //split
        String pageId = tokens[0].trim(); //get pageID

        String[] outlinks = tokens[1].trim().split(",");
        outlinksArrayList = new ArrayList();

        

        long j = PageToRow.get(pageId);
        remaining.remove(j); //remove all the pages on the left side of the adjacency list; This will give us number of dead nodes
        ctx.write(new Text(j + ",0,0,M"), NullWritable.get()); //for every
        
        //get the adjacency list (outlinks)
        for (String str : outlinks) {
            if (!"".equals(str.trim())) {
                outlinksArrayList.add(str.trim());
            }
        }
        if (!outlinksArrayList.isEmpty()) {
            for (String str : outlinksArrayList) {
                long i = PageToRow.get(str.trim());
                double d = 1 / (double) outlinksArrayList.size();
                ctx.write(new Text(i + "," + j + "," + d + ",M"), NullWritable.get()); // for every outlinks of the page create the M matrix
            }
        } else {
            ctx.write(new Text("0," + j + "," + pagerank + ",D"), NullWritable.get()); // if there are no elements in the outlinks put the entry in D matrix
        }
        
    }

    @Override
    public void cleanup(Context ctx) throws IOException, InterruptedException {
        // for all the dead nodes put entry in the D matrix
        for(long l:remaining){
            ctx.write(new Text("0," + l + "," + pagerank + ",D"), NullWritable.get());
        }
        mos.close();
    }

    }
