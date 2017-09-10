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


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// Just emit the lines to the reducer
// Reducer creates all the matrices (M,D,R)
public class MatrixMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    public void map(Object _k, Text lines, Context ctx) throws InterruptedException, IOException {
        ctx.write(lines, NullWritable.get());
  }

}
