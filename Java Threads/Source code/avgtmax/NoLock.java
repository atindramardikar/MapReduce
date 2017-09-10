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
public class NoLock {

    // Accumulation data structure to store stationID and running sum and count
    static Map<String, station> noLockAccumulation;
    
    //Data structure to store rumtimes for 10 executions
    static ArrayList<Long> noLockExectimes = new ArrayList<>();

    //execution of NO-LOCK
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public NoLock() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            noLockAccumulation = new HashMap();
            long start = System.currentTimeMillis();

            NoLockThread t1 = new NoLockThread(0, input.size() / 2); //Thread 1
            NoLockThread t2 = new NoLockThread(input.size() / 2, input.size()); //Thread 2

            t1.start();
            t2.start();

            t1.join();
            t2.join();
            long end = System.currentTimeMillis();

// Printed the final average values to evaluate results
//          Set<String> keySet = accumulation.keySet();
//              for (String key : keySet) {
//                station s = accumulation.get(key);
//                System.out.println(key + " : " + (s.sum / s.count));
//            }


            noLockExectimes.add(end - start);
        }
        Collections.sort(noLockExectimes);
        long sum = 0;
        for (long l : noLockExectimes) {
            sum += l;
        }

        System.out.println("Minimum for No Lock: " + noLockExectimes.get(0));
        System.out.println("Maximum for No Lock: " + noLockExectimes.get(9));
        System.out.println("Average for No Lock: " + sum / 10);
    }

    // update function to update the running sum and count
    public static void update(int index) {
        String str = input.get(index);
        if (str.contains("TMAX")) {
            String[] split = str.split(",");
            if (noLockAccumulation.get(split[0]) == null) {
                double count = 1;
                double sum = Integer.parseInt(split[3]);
                noLockAccumulation.put(split[0], new station(sum, count));
            } else {
                station s = noLockAccumulation.get(split[0]);
                s.sum += Double.parseDouble(split[3]);
                s.count++;
            }
        }
    }

    //execution of No-LOCK with Fibonacci
    public void noLockwithFib() throws InterruptedException {
        noLockExectimes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            noLockAccumulation = new HashMap();
            long start = System.currentTimeMillis();

            NoLockThreadforFib t1 = new NoLockThreadforFib(0, input.size() / 2);
            NoLockThreadforFib t2 = new NoLockThreadforFib(input.size() / 2, input.size());

            t1.start();
            t2.start();

            t1.join();
            t2.join();
            long end = System.currentTimeMillis();

//            Set<String> keySet = accumulation.keySet();
//            for (String key : keySet) {
//                station s = accumulation.get(key);
//                System.out.println(key + " : " + (s.sum / s.count));
//            }


            noLockExectimes.add(end - start);
        }
        Collections.sort(noLockExectimes);
        long sum = 0;
        for (long l : noLockExectimes) {
            sum += l;
        }

        System.out.println("Minimum for No Lock with Fibonacci: " + noLockExectimes.get(0));
        System.out.println("Maximum for No Lock with Fibonacci: " + noLockExectimes.get(9));
        System.out.println("Average for No Lock with Fibonacci: " + sum / 10);
    }

     // update function with Fibonacci to update the running sum and count
    public static void updatewithFib(int index) {
        String str = input.get(index);
        if (str.contains("TMAX")) {
            String[] split = str.split(",");
            if (noLockAccumulation.get(split[0]) == null) {
                double count = 1;
                double sum = Integer.parseInt(split[3]);
                noLockAccumulation.put(split[0], new station(sum, count));
            } else {
                station s = noLockAccumulation.get(split[0]);
                s.fibonacci(17); //call to fibonacci(17)
                s.sum += Double.parseDouble(split[3]);
                s.count++;
            }
        }
    }
}

// Thread class
class NoLockThread extends Thread {

    int min;
    int max;

    public NoLockThread(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            NoLock.update(i);
        }
    }
}

//Thread class for Fibonacci
class NoLockThreadforFib extends Thread {

    int min;
    int max;

    public NoLockThreadforFib(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        for (int i = min; i < max; i++) {
            NoLock.updatewithFib(i);
        }
    }
}
