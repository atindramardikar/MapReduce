package avgtmax;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author atindramardikar
 */
public class AvgTmax {

    static List<String> input = new ArrayList(); // Arraylist to store input (Lines of the files)

    // Main function which calls all versions (With and without Fibonacci)
    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException, Exception {

        loader(); // call Loader function
        Sequential seq = new Sequential(); //will execute the SEQ
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        NoLock nl = new NoLock(); //will execute the NO-LOCK
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        CoarseLock cl = new CoarseLock(); //will execute the COARSE-LOCK
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        FineLock fl = new FineLock(); //will execute the FINE-LOCK
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        NoSharing ns = new NoSharing(); //will execute the NO-SHARING
        System.out.println();
        System.out.println("----------------------With Fibonacci----------------------");
        System.out.println();
        seq.sequntialWithFib(); //will execute the SEQ with fibonacci
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        nl.noLockwithFib(); //will execute the NO-LOCK with fibonacci
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        cl.coarseLockWithFib(); //will execute the COARSE-LOCK with fibonacci
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        fl.fineLockWithFib(); //will execute the FINE-LOCK with fibonacci
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println();
        ns.NoSharingWithFib(); //will execute the NO-SHARING with fibonacci
    }

    
    // loader function: reads the input file line by line and stores it in the input Arraylist
    public static void loader() throws FileNotFoundException, IOException {
        FileReader fr = new FileReader("1868.csv"); // reading the file
        BufferedReader br = new BufferedReader(fr);
        String str;
        int k = 0;
        try {
            while ((str = br.readLine()) != null) {
                input.add(str);
            }
            br.close();
        } catch (Exception e) {
        }

    }
}


// station class: for every station there is a station class which stores its running sum and count
class station {

    double sum;
    double count;

    public station(Double sum, Double count) {
        this.sum = sum;
        this.count = count;
    }

    // function for FINE-LOCK execution
    public synchronized void setValuesforFineLock(double oldsum) {
        this.sum += oldsum;
        this.count++;
    }
    
    // function for Fine-LOCK with fibonacci execution
    public synchronized void setValuesforFineLockwithFib(double oldsum) {
        int n=fibonacci(17);
        this.sum += oldsum;
        this.count++;
    }

    // fibonacci function to calculate fibonacci(17) in the fibonacci programs.
    public int fibonacci(int n) {
        if (n == 1 || n == 2) {
            return 1;
        }
        int fibo1 = 1, fibo2 = 1, fibonacci = 1;
        for (int i = 3; i <= n; i++) {
            fibonacci = fibo1 + fibo2; // Fibonacci number is sum of previous two Fibonacci number
            fibo1 = fibo2;
            fibo2 = fibonacci;

        }
        return fibonacci; // Fibonacci number
    }
}
