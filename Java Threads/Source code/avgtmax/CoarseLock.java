package avgtmax;

import static avgtmax.AvgTmax.input;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author atindramardikar
 */
public class CoarseLock {

    // Accumulation data structure to store stationID and running sum and count
    static Map<String, station> coarseLockAccumulation;
    
    //Data structure to store rumtimes for 10 executions
    static ArrayList<Long> coarseLockExectimes = new ArrayList<>();

    //execution of COARSE-LOCK
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public CoarseLock() throws Exception {

        for (int i = 0; i < 10; i++) {

            coarseLockAccumulation = new HashMap();

            long start = System.currentTimeMillis();

            CoarseLockThread t1 = new CoarseLockThread(0, input.size() / 2); //Thread 1
            CoarseLockThread t2 = new CoarseLockThread(input.size() / 2, input.size()); //Thread 2

            t1.start();
            t2.start();

            t1.join();
            t2.join();
            long end = System.currentTimeMillis();

// Printed the final average values to evaluate results
//        Set<String> keySet = accumulation.keySet();
//        for (String key : keySet) {
//            station s = accumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

            coarseLockExectimes.add(end - start);
        }
        Collections.sort(coarseLockExectimes);
        long sum = 0;
        for (long l : coarseLockExectimes) {
            sum += l;
        }

        System.out.println("Minimum for Coarse Lock: " + coarseLockExectimes.get(0));
        System.out.println("Maximum for Coarse Lock: " + coarseLockExectimes.get(9));
        System.out.println("Average for Coarse Lock: " + sum / 10);

    }

    // update function to update the running sum and count
    // synchronized so only one thread can access this once
    public static synchronized void update(int index) {
        String str = input.get(index);
        if (str.contains("TMAX")) {
            String[] split = str.split(",");
            if (coarseLockAccumulation.get(split[0]) == null) {
                double count = 1;
                double sum = Integer.parseInt(split[3]);
                coarseLockAccumulation.put(split[0], new station(sum, count));
            } else {
                station s = coarseLockAccumulation.get(split[0]);
                s.sum += Double.parseDouble(split[3]);
                s.count++;
            }
        }
    }

    //execution of COARSE-LOCK with Fibonacci
    public void coarseLockWithFib() throws Exception {
        coarseLockExectimes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {

            coarseLockAccumulation = new HashMap();

            long start = System.currentTimeMillis();

            CoarseLockThreadforFib t1 = new CoarseLockThreadforFib(0, input.size() / 2);
            CoarseLockThreadforFib t2 = new CoarseLockThreadforFib(input.size() / 2, input.size());

            t1.start();
            t2.start();

            t1.join();
            t2.join();
            long end = System.currentTimeMillis();

//        Set<String> keySet = accumulation.keySet();
//        for (String key : keySet) {
//            station s = accumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

            coarseLockExectimes.add(end - start);
        }
        Collections.sort(coarseLockExectimes);
        long sum = 0;
        for (long l : coarseLockExectimes) {
            sum += l;
        }

        System.out.println("Minimum for Coarse Lock with Fibonacci: " + coarseLockExectimes.get(0));
        System.out.println("Maximum for Coarse Lock with Fibonacci: " + coarseLockExectimes.get(9));
        System.out.println("Average for Coarse Lock with Fibonacci: " + sum / 10);

    }

    public static synchronized void updateWithFib(int index) {
        String str = input.get(index);
        if (str.contains("TMAX")) {
            String[] split = str.split(",");
            if (coarseLockAccumulation.get(split[0]) == null) {
                double count = 1;
                double sum = Integer.parseInt(split[3]);
                coarseLockAccumulation.put(split[0], new station(sum, count));
            } else {
                station s = coarseLockAccumulation.get(split[0]);
                int n = s.fibonacci(17);
                s.sum += Double.parseDouble(split[3]);
                s.count++;
            }
        }
    }
}

// Thread Class
class CoarseLockThread extends Thread {

    int min;
    int max;

    public CoarseLockThread(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            CoarseLock.update(i);
        }
    }
}

//Thread Class for Fibonacci
class CoarseLockThreadforFib extends Thread {

    int min;
    int max;

    public CoarseLockThreadforFib(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            CoarseLock.updateWithFib(i);
        }
    }
}
