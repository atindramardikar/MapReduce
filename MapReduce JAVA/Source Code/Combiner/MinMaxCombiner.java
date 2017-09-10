/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package minmaxcombiner;


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
public class MinMaxCombiner {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Minmax Combiner");
        job.setJarByClass(MinMaxCombiner.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class); //Use of Combiner Class
        job.setReducerClass(AvgReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MeanCombiner.class);
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
// The output of the Mapper is key-> StationId Value -> Object of MeanCombiner Class
   class TokenizerMapper
            extends Mapper<Object, Text, Text, MeanCombiner> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split("\n");
            Double one= 1.0;
            for (int i = 0; i < temp.length; i++) {
                String[] tokens = temp[i].split(",");
                String stationId = tokens[0];
                if ("TMAX".equals(tokens[2])) {
                    context.write(new Text(stationId), new MeanCombiner(Double.NEGATIVE_INFINITY, Double.parseDouble(tokens[3]), one));
                }
                if ("TMIN".equals(tokens[2])) {
                    context.write(new Text(stationId), new MeanCombiner(Double.parseDouble(tokens[3]), Double.POSITIVE_INFINITY,one));
                }

            }
        }
    }

// Combiner: For every Map call we try to combine the Tmin and Tmax values with 
// their count for each station 
class Combiner 
            extends Reducer<Text, MeanCombiner,Text,MeanCombiner>{
    
    public void reduce(Text key, Iterable<MeanCombiner> values,
                Context context) throws IOException, InterruptedException {
            double TminSum = 0;
            double TmaxSum = 0;
            double TminCount = 0;
            double TmaxCount = 0;
            for (MeanCombiner val : values) {
                if(val.Tmax!= Double.POSITIVE_INFINITY){
                    TmaxSum+=val.Tmax;
                    TmaxCount+=val.count;
                }
                else if(val.Tmin!= Double.NEGATIVE_INFINITY){
                    TminSum+=val.Tmin;
                    TminCount+=val.count;
                }
            }
            
            context.write(key, new MeanCombiner(TminSum,Double.POSITIVE_INFINITY,TminCount));
            context.write(key, new MeanCombiner(Double.NEGATIVE_INFINITY,TmaxSum,TmaxCount));
        }
}

// Reducer: Takes the min and max temp values for each StationID and emits the average
    class AvgReducer
            extends Reducer<Text, MeanCombiner, Text, Text> {


        public void reduce(Text key, Iterable<MeanCombiner> values,
                Context context) throws IOException, InterruptedException {
            double TminCount = 0.0;
            double TmaxCount = 0.0;
            double TminSum = 0.0;
            double TmaxSum = 0.0;
            for (MeanCombiner val : values) {
                if(val.Tmax!= Double.POSITIVE_INFINITY){
                    TmaxCount+=val.count;
                    TmaxSum+=val.Tmax;
                }
                else if(val.Tmin!= Double.NEGATIVE_INFINITY){
                    TminCount+=val.count;
                    TminSum+=val.Tmin;
                }
            }
            double meanTmin= TminSum/TminCount;
            double meanTmax= TmaxSum/TmaxCount;
            context.write(key, new Text(meanTmin+","+meanTmax));
        }
    }

// MeanCombiner to store Tmax or Tmin for a Station along with its count
// We require a count variable as we want to keep track of the count as we 
// sum up the values in combining stage.
class MeanCombiner implements Writable {

    public double Tmin;
    public double Tmax;
    public double count; // needed for combiner

    public MeanCombiner() {
    }

    public MeanCombiner(double Tmin, double Tmax,double count) {
        this.Tmin = Tmin;
        this.Tmax = Tmax;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(Tmin);
        out.writeDouble(Tmax);
        out.writeDouble(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.Tmin = in.readDouble();
        this.Tmax = in.readDouble();
        this.count = in.readDouble();
    }
}

