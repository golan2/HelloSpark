package golan.hello.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.InetAddress;
import java.util.List;

/**
 * Created by golaniz on 03/02/2016.
 */
public class SimpleRead extends AbsSpark {

    public static void main(String[] args) {
        JavaSparkContext context = null;
        try {
            System.out.println("FQDN: " + InetAddress.getLocalHost().getHostName());
            System.out.println("Thread: " + Thread.currentThread().getName());
            System.out.println("QQ: Create context...");
            context = getJavaSparkContext(SimpleRead.class.getSimpleName());
            System.out.println("QQ: Read file...");
            JavaRDD<String> rdd = context.textFile(Utils.findFileInClasspath(PAIR_RDD_FUNCTIONS_TXT));
            List<String> list = rdd.collect();
            System.out.println("QQ: Calc total...");
            int total = 0;
            for (String s : list) {
                total+=s.length();
            }
            System.out.println("QQ: Total = " + total);
        } catch (Exception e) {
            if (context!=null) context.close();
        }
    }
}
