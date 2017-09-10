package minmaxtemp;

import java.io.DataInput;
import java.io.DataOutput;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author atindramardikar
 */
public class MinMaxTemp {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "MinMax");
        job.setJarByClass(MinMaxTemp.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class); //The combiner is commented out for this version.
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}

// Mapper: Reads the file, Splits by line then each line by tokens
// There is a class Mean to store information about the Station and min/max values
// The output of the Mapper is key-> StationId Value -> Object of Mean Class
   class TokenizerMapper
            extends Mapper<Object, Text, Text, Mean> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split("\n");// Breaking by line
            for (int i = 0; i < temp.length; i++) {
                String[] tokens = temp[i].split(","); //breaking each line into tokens
                String stationId = tokens[0];
                if ("TMAX".equals(tokens[2])) {
                    context.write(new Text(stationId), new Mean(Double.NEGATIVE_INFINITY, Double.parseDouble(tokens[3])));
                }
                if ("TMIN".equals(tokens[2])) {
                    context.write(new Text(stationId), new Mean(Double.parseDouble(tokens[3]), Double.POSITIVE_INFINITY));
                }

            }
        }
    }

// Reducer: Takes the min and max temp values for each StationID and emits the average

    class IntSumReducer
            extends Reducer<Text, Mean, Text, Text> {

        public void reduce(Text key, Iterable<Mean> values,
                Context context) throws IOException, InterruptedException {
            // local variables to store sum and count for min and max temps
            double TminCount = 0;
            double TmaxCount = 0;
            double TminSum = 0;
            double TmaxSum = 0;
            for (Mean val : values) {
                if(val.Tmax!= Double.POSITIVE_INFINITY){
                    TmaxCount++;
                    TmaxSum+=val.Tmax;
                }
                else if(val.Tmin!= Double.NEGATIVE_INFINITY){
                    TminCount++;
                    TminSum+=val.Tmin;
                }
            }
            double meanTmin= TminSum/TminCount;
            double meanTmax= TmaxSum/TmaxCount;
            if(TmaxCount==0){
                meanTmax=0;
            }
            context.write(key, new Text(meanTmin+","+meanTmax));
        }
    }

//Mean: To store min or max values for each station
class Mean implements Writable {

    public double Tmin;
    public double Tmax;

    public Mean() {
    }

    public Mean(double Tmin, double Tmax) {
        this.Tmin = Tmin;
        this.Tmax = Tmax;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(Tmin);
        out.writeDouble(Tmax);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.Tmin = in.readDouble();
        this.Tmax = in.readDouble();
    }
}
