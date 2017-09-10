package secondarysort;

import java.util.HashMap;
import java.util.Map;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySort {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Secondary Sort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(AvgReducer.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setMapOutputKeyClass(StationIdAndYear.class);
        job.setMapOutputValueClass(Station.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//Group the records based on the stationID.
// So while sorting just considers stationID so two keys with different years are considered identical.
class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(StationIdAndYear.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        StationIdAndYear key1 = (StationIdAndYear) w1;
        StationIdAndYear key2 = (StationIdAndYear) w2;

        return key1.getStationID().compareTo(key2.getStationID());

    }
}

//Custom implementation of WritableComparator to sort the values by comparing StationId.
//If stationiD is equal sorts it by Year
class KeyComparator extends WritableComparator {

    protected KeyComparator() {
        super(StationIdAndYear.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        StationIdAndYear key1 = (StationIdAndYear) w1;
        StationIdAndYear key2 = (StationIdAndYear) w2;

        int cmp = key1.getStationID().compareTo(key2.getStationID());

        if (cmp != 0) {
            return cmp;
        }
        return key1.getYear().compareTo(key2.getYear());
    }
}

// Mapper: Reads the file, Splits by line then each line by tokens
// We store information in Custom key and update the information in customKeyclass
class TokenizerMapper extends Mapper<Object, Text, StationIdAndYear, Station> {

    Map<StationIdAndYear, Station> stationInfo;

    @Override
    public void setup(Mapper.Context context) {
        stationInfo = new HashMap<StationIdAndYear, Station>();
    }

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("\n");

        for (String line : lines) {

            if (line.contains("TMAX") || line.contains("TMIN")) {
                String[] recordComponents = line.split(",");
                String stationID = recordComponents[0];
                Integer year = Integer.parseInt(recordComponents[1].substring(0, 4));
                StationIdAndYear customMapKey = new StationIdAndYear(stationID, year);

                if (recordComponents[2].equals("TMAX")) {

                    if (stationInfo.containsKey(customMapKey)) {
                        stationInfo.get(customMapKey).updateTmax(Double.parseDouble(recordComponents[3]));
                    } else {
                        stationInfo.put(customMapKey, new Station(Double.parseDouble(recordComponents[3]), 0.0, 1, 0));
                    }
                }
                else if (recordComponents[2].equals("TMIN")) {
                    if (stationInfo.containsKey(customMapKey)) {
                        stationInfo.get(customMapKey).updateTmin(Double.parseDouble(recordComponents[3]));
                    } else {
                        stationInfo.put(customMapKey, new Station(0.0, Double.parseDouble(recordComponents[3]), 0, 1));
                    }
                }
            }
        }
    }

    @Override
    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        for (StationIdAndYear st : stationInfo.keySet()) {
            context.write(st, stationInfo.get(st));
        }
    }
}

//takes in the key pair and station info and emits the desired output.
class AvgReducer extends Reducer<StationIdAndYear, Station, Text, NullWritable> {
    
    private NullWritable nullValue = NullWritable.get();

    @Override
    public void reduce(StationIdAndYear key, Iterable<Station> values, Context context) throws IOException, InterruptedException {

        double Tmin = 0;
        double Tmax = 0;
        int TmaxCount = 0;
        int TminCount = 0;
        StringBuilder result = new StringBuilder();
        Map<Integer, Station> stationByYearDetails = new LinkedHashMap<>();

        for (Station station : values) {

            Tmin = station.getTmin();
            Tmax = station.getTmax();
            TmaxCount = station.getCountTmax();
            TminCount = station.getCountTmin();

            if (stationByYearDetails.containsKey(key.getYear())) {
                stationByYearDetails.get(key.getYear()).update(Tmax, Tmin, TmaxCount, TminCount);
            } else {
                stationByYearDetails.put(key.getYear(), new Station(Tmax, Tmin, TmaxCount, TminCount));
            }
        }

// format Rusult using stringbuilder
        result.append(key.getStationID() + ", [");
        int count = stationByYearDetails.size();
        for (Integer i : stationByYearDetails.keySet()) {
            if (count == 1) {
                result.append("(")
                        .append("" + i)
                        .append(", ")
                        .append((stationByYearDetails.get(i).getCountTmin() == 0) ? "0.0" : "" + stationByYearDetails.get(i).getTmin() / stationByYearDetails.get(i).getCountTmin())
                        .append(", ")
                        .append((stationByYearDetails.get(i).getCountTmax() == 0) ? "0.0" : "" + stationByYearDetails.get(i).getTmax() / stationByYearDetails.get(i).getCountTmax())
                        .append(")]");
            } else {
                result.append("(")
                        .append("" + i)
                        .append(", ")
                        .append((stationByYearDetails.get(i).getCountTmin() == 0) ? "0.0" : "" + stationByYearDetails.get(i).getTmin() / stationByYearDetails.get(i).getCountTmin())
                        .append(", ")
                        .append((stationByYearDetails.get(i).getCountTmax() == 0) ? "0.0" : "" + stationByYearDetails.get(i).getTmax() / stationByYearDetails.get(i).getCountTmax())
                        .append("),");
            }
            count--;
        }
        context.write(new Text(result.toString()), nullValue);
    }
}

//Station: To store min or max values for each station
class Station implements Writable {

    private Double Tmax;
    private Double Tmin;
    private Integer TmaxCount;
    private Integer TminCount;

    public Station() {
        // need this for serialization
    }

    public Double getTmax() {
        return Tmax;
    }

    public Double getTmin() {
        return Tmin;
    }
    
    public Integer getCountTmax() {
        return TmaxCount;
    }

    public Integer getCountTmin() {
        return TminCount;
    }

    public Station(Double tmax, Double tmin, Integer countTmax, Integer countTmin) {
        super();
        this.Tmax = tmax;
        this.Tmin = tmin;
        this.TmaxCount = countTmax;
        this.TminCount = countTmin;
    }
    
    public void update(Double tmax, Double tmin, Integer countTmax, Integer countTmin){
        this.Tmax = tmax;
        this.Tmin = tmin;
        this.TmaxCount = countTmax;
        this.TminCount = countTmin;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.Tmax = in.readDouble();
        this.Tmin = in.readDouble();
        this.TmaxCount = in.readInt();
        this.TminCount = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(Tmax);
        out.writeDouble(Tmin);
        out.writeInt(TmaxCount);
        out.writeInt(TminCount);
    }

    @Override
    public int hashCode() {
        return (int) new Double(Tmax).hashCode()
                + (int) new Double(Tmin).hashCode()
                + new Integer(TmaxCount).hashCode()
                + new Integer(TminCount).hashCode();
    }

    public void updateTmin(Double temp) {
        this.Tmin = this.Tmin + temp;
        TminCount++;
    }

    public void updateTmax(Double temp) {
        this.Tmax = this.Tmax + temp;
        TmaxCount++;
    }
}


//StationIdAndYear is a custom data type used as key in mapper and reducer phase
//StationIdAndYear is a combination of stationId and year.
class StationIdAndYear implements WritableComparable {

    private String stationID;
    private Integer year;

    public StationIdAndYear() {
    }

    public StationIdAndYear(String stationID, Integer year) {
        super();
        this.stationID = stationID;
        this.year = year;
    }

    public String getStationID() {
        return stationID;
    }

    public Integer getYear() {
        return year;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.year = in.readInt();
        this.stationID = in.readUTF();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(year);
        out.writeUTF(stationID);

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((stationID == null) ? 0 : stationID.hashCode());
        result = prime * result + ((year == null) ? 0 : year.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StationIdAndYear other = (StationIdAndYear) obj;
        if (stationID == null) {
            if (other.stationID != null) {
                return false;
            }
        } else if (!stationID.equals(other.stationID)) {
            return false;
        }
        if (year == null) {
            if (other.year != null) {
                return false;
            }
        } else if (!year.equals(other.year)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Object o) {
        StationIdAndYear other = (StationIdAndYear) o;
        if (this.stationID.equals(other.stationID)) {
            return this.year.compareTo(other.year);
        } else {
            return this.stationID.compareTo(other.stationID);
        }
    }
}