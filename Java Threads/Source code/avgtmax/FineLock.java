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
public class FineLock {

    // Accumulation data structure to store stationID and running sum and count
    static Map<String, station> fineLockAccumulation;
    
    //Data structure to store rumtimes for 10 executions
    static ArrayList<Long> fineLockExectimes = new ArrayList<>();

    //execution of FINE-LOCK
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public FineLock() throws Exception {


        for (int i = 0; i < 10; i++) {

            fineLockAccumulation = new HashMap();

            long start = System.currentTimeMillis();

            FineLockThread t1 = new FineLockThread(0, input.size() / 2); //Thread 1
            FineLockThread t2 = new FineLockThread(input.size() / 2, input.size());//Thread 2

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            long end = System.currentTimeMillis();

//        Set<String> keySet = accumulation.keySet();
//        for (String key : keySet) {
//            stationFineLock s = accumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

            fineLockExectimes.add(end - start);
        }
        Collections.sort(fineLockExectimes);
        long sum = 0;
        for (long l : fineLockExectimes) {
            sum += l;
        }

        System.out.println("Minimum for Fine Lock: " + fineLockExectimes.get(0));
        System.out.println("Maximum for Fine Lock: " + fineLockExectimes.get(9));
        System.out.println("Average for Fine Lock: " + sum / 10);

    }
    
    // update function to update the running sum and count
    public static void update(int index) {
        String str = input.get(index);
        if (str.contains("TMAX")) {
            String[] split = str.split(",");
            if (fineLockAccumulation.get(split[0]) == null) {
                double count = 1;
                double sum = Integer.parseInt(split[3]);
                fineLockAccumulation.put(split[0], new station(sum, count));
            } else {
                station s = fineLockAccumulation.get(split[0]);
                s.setValuesforFineLock(Double.parseDouble(split[3])); //This function is synchronized so only the values are locked.
            }
        }
    }

    //execution of FINE-LOCK with Fibonacci
    public void fineLockWithFib() throws Exception {
        fineLockExectimes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {

            fineLockAccumulation = new HashMap();

            long start = System.currentTimeMillis();

            FineLockThreadWithFib t1 = new FineLockThreadWithFib(0, input.size() / 2);
            FineLockThreadWithFib t2 = new FineLockThreadWithFib(input.size() / 2, input.size());

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            long end = System.currentTimeMillis();

//        Set<String> keySet = accumulation.keySet();
//        for (String key : keySet) {
//            stationFineLock s = accumulation.get(key);
//            System.out.println(key + " : " + (s.sum / s.count));
//        }

            fineLockExectimes.add(end - start);
        }
        Collections.sort(fineLockExectimes);
        long sum = 0;
        for (long l : fineLockExectimes) {
            sum += l;
        }

        System.out.println("Minimum for Fine Lock with Fibonacci: " + fineLockExectimes.get(0));
        System.out.println("Maximum for Fine Lock with Fibonacci: " + fineLockExectimes.get(9));
        System.out.println("Average for Fine Lock with Fibonacci: " + sum / 10);

    }

    public static void updateWithFib(int index) {
        String str = input.get(index);
        if (str.contains("TMAX")) {
            String[] split = str.split(",");
            if (fineLockAccumulation.get(split[0]) == null) {
                double count = 1;
                double sum = Integer.parseInt(split[3]);
                fineLockAccumulation.put(split[0], new station(sum, count));
            } else {
                station s = fineLockAccumulation.get(split[0]);
                s.setValuesforFineLockwithFib(Double.parseDouble(split[3]));
            }
        }
    }
}

//Thread Class
class FineLockThread extends Thread {

    int min;
    int max;

    public FineLockThread(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            FineLock.update(i);
        }
    }
}


//Thread Class for Fibonacci
class FineLockThreadWithFib extends Thread {

    int min;
    int max;

    public FineLockThreadWithFib(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            FineLock.updateWithFib(i);
        }
    }
}
