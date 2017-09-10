package avgtmax;

import static avgtmax.AvgTmax.input;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author atindramardikar
 */
public class Sequential {

    // Accumulation data structure to store stationID and running sum and count
    static Map<String, station> seqAccumulation;
    
    //Data structure to store rumtimes for 10 executions
    static ArrayList<Long> seqExectimes = new ArrayList<>();

    //execution of sequential
    public Sequential() {
        for (int i = 0; i < 10; i++) {
            seqAccumulation = new HashMap();
            long start = System.currentTimeMillis(); //start of program
            for (String str : input) {
                if (str.contains("TMAX")) {
                    String[] split = str.split(",");
                    if (seqAccumulation.get(split[0]) == null) {
                        double count = 1;
                        double sum = Integer.parseInt(split[3]);
                        seqAccumulation.put(split[0], new station(sum, count));
                    } else {
                        station s = seqAccumulation.get(split[0]);
                        s.sum += Double.parseDouble(split[3]);
                        s.count++;
                    }
                }
            }
            
// Printed the final average values to evaluate results

//        Set<String> keySet = seqAccumulation.keySet();
//        for (String key : keySet) {
//            station s = seqAccumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

            long end = System.currentTimeMillis(); //end of program
            seqExectimes.add(end - start); //execution time
        }
        Collections.sort(seqExectimes);
        long sum = 0;
        for (long l : seqExectimes) {
            sum += l;
        }

        System.out.println("Minimum for Sequential: " + seqExectimes.get(0));
        System.out.println("Maximum for Sequential: " + seqExectimes.get(9));
        System.out.println("Average for Sequential: " + sum / 10);
    }

    //execution of sequential with Fibonacci
    public void sequntialWithFib() {
        ArrayList<Long> seqExectimesForFib = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            seqAccumulation = new HashMap();
            long start = System.currentTimeMillis();
            for (String str : input) {
                if (str.contains("TMAX")) {
                    String[] split = str.split(",");
                    if (seqAccumulation.get(split[0]) == null) {
                        double count = 1;
                        double sum = Integer.parseInt(split[3]);
                        seqAccumulation.put(split[0], new station(sum, count));
                    } else {
                        station s = seqAccumulation.get(split[0]);
                        int n = s.fibonacci(17); //call to fibonacci(17)
                        s.sum += Double.parseDouble(split[3]);
                        s.count++;
                    }
                }
            }

//        Set<String> keySet = seqAccumulation.keySet();
//        for (String key : keySet) {
//            station s = seqAccumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

            long end = System.currentTimeMillis();
            seqExectimesForFib.add(end - start);
        }
        Collections.sort(seqExectimesForFib);
        long sum = 0;
        for (long l : seqExectimesForFib) {
            sum += l;
        }

        System.out.println("Minimum for Sequential with Fibonacci: " + seqExectimesForFib.get(0));
        System.out.println("Maximum for Sequential with Fibonacci: " + seqExectimesForFib.get(9));
        System.out.println("Average for Sequential with Fibonacci: " + sum / 10);
    }
}
