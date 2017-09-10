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
public class NoSharing {

    // Accumulation data structure to store stationID and running sum and count
    static Map<String, station> totalAccumulation;
    
    //Data structure to store rumtimes for 10 executions
    static ArrayList<Long> noSharingExectimes = new ArrayList<>();

    //execution of NO-SHARING
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public NoSharing() throws Exception {

        for (int i = 0; i < 10; i++) {

            totalAccumulation = new HashMap();

            long start = System.currentTimeMillis();
            NoSharingThread t1 = new NoSharingThread(0, input.size() / 2); //Thread 1
            NoSharingThread t2 = new NoSharingThread(input.size() / 2, input.size()); //Thread 2

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            long end = System.currentTimeMillis();
            
            // Combining accumulation data structure from ann thread class to one.
            totalAccumulation.putAll(t1.accumulation);

            Set<String> keySet1 = t2.accumulation.keySet();
            for (String key : keySet1) {
                if (totalAccumulation.get(key) == null) {
                    totalAccumulation.put(key, t2.accumulation.get(key));
                } else {
                    station s = t2.accumulation.get(key);
                    double sum = s.sum + totalAccumulation.get(key).sum;
                    double count = s.count + totalAccumulation.get(key).count;
                    totalAccumulation.put(key, new station(sum, count));
                }
            }

            noSharingExectimes.add(end - start);

        }
        Collections.sort(noSharingExectimes);

//        Set<String> keySet = totalAccumulation.keySet();
//        for (String key : keySet) {
//            stationNoSharing s = totalAccumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

        long sum = 0;
        for (long l : noSharingExectimes) {
            sum += l;
        }

        System.out.println("Minimum for No Sharing: " + noSharingExectimes.get(0));
        System.out.println("Maximum for No Sharing: " + noSharingExectimes.get(9));
        System.out.println("Average for No Sharing: " + sum / 10);

    }

    //execution of NO-SHARING with Fibonacci
    public void NoSharingWithFib() throws Exception {
        noSharingExectimes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {

            totalAccumulation = new HashMap();

            long start = System.currentTimeMillis();
            NoSharingThreadwithFib t1 = new NoSharingThreadwithFib(0, input.size() / 2);
            NoSharingThreadwithFib t2 = new NoSharingThreadwithFib(input.size() / 2, input.size());

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            long end = System.currentTimeMillis();

            // Combining accumulation data structure from ann thread class to one.
            totalAccumulation.putAll(t1.accumulation);

            Set<String> keySet1 = t2.accumulation.keySet();
            for (String key : keySet1) {
                if (totalAccumulation.get(key) == null) {
                    totalAccumulation.put(key, t2.accumulation.get(key));
                } else {
                    station s = t2.accumulation.get(key);
                    double sum = s.sum + totalAccumulation.get(key).sum;
                    double count = s.count + totalAccumulation.get(key).count;
                    totalAccumulation.put(key, new station(sum, count));
                }
            }

            noSharingExectimes.add(end - start);

        }
        Collections.sort(noSharingExectimes);

//        Set<String> keySet = totalAccumulation.keySet();
//        for (String key : keySet) {
//            stationNoSharing s = totalAccumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

        long sum = 0;
        for (long l : noSharingExectimes) {
            sum += l;
        }

        System.out.println("Minimum for No Sharing with Fibonacci: " + noSharingExectimes.get(0));
        System.out.println("Maximum for No Sharing with Fibonacci: " + noSharingExectimes.get(9));
        System.out.println("Average for No Sharing with Fibonacci: " + sum / 10);

    }
}

//Thread Class
class NoSharingThread extends Thread {

    int min;
    int max;
    static Map<String, station> accumulation = new HashMap();// accumulation data structure for all Thread class

    public NoSharingThread(int min, int max) {
        this.min = min;
        this.max = max;
    }

    // updating their own accumulation in the run
    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            String str = input.get(i);
            if (str.contains("TMAX")) {
                String[] split = str.split(",");
                if (accumulation.get(split[0]) == null) {
                    double count = 1;
                    double sum = Integer.parseInt(split[3]);
                    accumulation.put(split[0], new station(sum, count));
                } else {
                    station s = accumulation.get(split[0]);
                    //s.setValues(Double.parseDouble(split[3]));
                    s.sum += Double.parseDouble(split[3]);
                    s.count++;
                }
            }
        }

    }
}


//THread Class for Fibonacci
class NoSharingThreadwithFib extends Thread {

    int min;
    int max;
    static Map<String, station> accumulation = new HashMap();

    public NoSharingThreadwithFib(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            String str = input.get(i);
            if (str.contains("TMAX")) {
                String[] split = str.split(",");
                if (accumulation.get(split[0]) == null) {
                    double count = 1;
                    double sum = Integer.parseInt(split[3]);
                    accumulation.put(split[0], new station(sum, count));
                } else {
                    station s = accumulation.get(split[0]);
                    //s.setValues(Double.parseDouble(split[3]));
                    int n = s.fibonacci(17);
                    s.sum += Double.parseDouble(split[3]);
                    s.count++;
                }
            }
        }

    }
}
