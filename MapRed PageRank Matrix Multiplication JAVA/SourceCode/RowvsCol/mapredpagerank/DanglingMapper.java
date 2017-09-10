package mapredpagerank;

/**
 *
 * @author atindramardikar
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class DanglingMapper extends Mapper<Object, Text, LongWritable, PageNode> {

    @Override
    public void map(Object _k, Text lines, Context ctx) throws IOException, InterruptedException {
         String line=lines.toString();
         String[]tokens=line.split(",");
         long row=Long.parseLong(tokens[0]);
         long col=Long.parseLong(tokens[1]);
         double val=Double.parseDouble(tokens[2]);
         String id=tokens[3];
         if("D".equals(id.trim())){
            ctx.write(new LongWritable(row),new PageNode(val,col,id));
         }
        
    }
}
