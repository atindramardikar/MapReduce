/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package minmaxinmap;

import java.io.DataInput;
import java.io.DataOutput;


import java.io.IOException;
import java.util.HashMap;
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
public class MinMaxInMap {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MinMaxInMap.class);
        job.setMapperClass(TokenizerMapper.class);
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
// There is a class MeanCombiner to store information about the Station and min/max values
// The output of the Mapper is key-> StationId Value -> Object of MeanCombiner Class
class TokenizerMapper
        extends Mapper<Object, Text, Text, MeanCombiner> {

    private HashMap<String, Double[]> TminMap;//To store combined Tmin values for every station
    private HashMap<String, Double[]> TmaxMap;//To store combined Tmax values for every station

    public void setup(Context context) {
        TminMap = new HashMap<>();
        TmaxMap = new HashMap<>();
    }

    // We store each station Id in key of the Tmon and Tmax maps and update their values
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] temp = value.toString().split("\n");
        Double one = 1.0;
        for (int i = 0; i < temp.length; i++) {
            String[] tokens = temp[i].split(",");
            String stationId = tokens[0];
            if ("TMAX".equals(tokens[2])) {
                if (!TmaxMap.containsKey(stationId)) {
                    Double array[] = {Double.parseDouble(tokens[3]), 1.0};
                    TmaxMap.put(stationId, array);
                } else {
                    Double tempArr[] = TmaxMap.get(stationId);
                    tempArr[0] += Double.parseDouble(tokens[3]);
                    tempArr[1] += 1;
                    TmaxMap.put(stationId, tempArr);
                }

            }
            if ("TMIN".equals(tokens[2])) {
                if (!TminMap.containsKey(stationId)) {
                    Double array[] = {Double.parseDouble(tokens[3]), 1.0};
                    TminMap.put(stationId, array);
                } else {
                    Double tempArr[] = TminMap.get(stationId);
                    tempArr[0] += Double.parseDouble(tokens[3]);
                    tempArr[1] += 1;
                    TminMap.put(stationId, tempArr);
                }
            }

        }
    }

    // Finally we emit the combined value for that map call.
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String key : TminMap.keySet()) {
            context.write(new Text(key), new MeanCombiner(TminMap.get(key)[0], Double.POSITIVE_INFINITY, TminMap.get(key)[1]));
        }
        for (String key : TmaxMap.keySet()) {
            context.write(new Text(key), new MeanCombiner(Double.NEGATIVE_INFINITY, TmaxMap.get(key)[0], TmaxMap.get(key)[1]));
        }
    }
}

// Reducer: Takes the min and max temp values for each StationID and emits the average
class AvgReducer
        extends Reducer<Text, MeanCombiner, Text, Text> {

    public void reduce(Text key, Iterable<MeanCombiner> values,
            Context context) throws IOException, InterruptedException {
        double TminCount = 0;
        double TmaxCount = 0;
        double TminSum = 0;
        double TmaxSum = 0;
        for (MeanCombiner val : values) {
            if (val.Tmax != Double.POSITIVE_INFINITY) {
                TmaxCount += val.count;
                TmaxSum += val.Tmax;
            } else if (val.Tmin != Double.NEGATIVE_INFINITY) {
                TminCount += val.count;
                TminSum += val.Tmin;
            }
        }
        double meanTmin = TminSum / TminCount;
        double meanTmax = TmaxSum / TmaxCount;
        context.write(key, new Text(meanTmin + "," + meanTmax));
    }
}

// MeanCombiner to store Tmax or Tmin for a Station along with its count
// We require a count variable as we want to keep track of the count as we 
// sum up the values in combining stage (in Map combiner).
class MeanCombiner implements Writable {

    public double Tmin;
    public double Tmax;
    public double count;

    public MeanCombiner() {
    }

    public MeanCombiner(double Tmin, double Tmax, double count) {
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
