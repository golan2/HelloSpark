package golan.hello.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by golaniz on 20/01/2016.
 */
public class ReadFiles extends AbsSpark {

    private static final int SLEEP_TIME = 15000;
    private static boolean waiting = true;
    private static List<Integer> list = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================== BEGIN ====================================");
        new R().run();
        R r = new R();
        new Thread(r, "IZIK_ReadFiles_R").start();
//        Ex ex = new Ex();
//        new Thread(ex, "IZIK_ReadFiles_Ex").start();

        while (!r.finished) {
            Thread.sleep(SLEEP_TIME*2);
        }

        System.out.println("========================== E N D ====================================");
    }

    private static class R implements Runnable {
        boolean finished = false;
        @Override
        public void run() {
            JavaSparkContext context = null;
            try {
//                System.out.println("R: Waiting... ");
//                byte[] b = new byte[100];
//                while (waiting) {
//                    System.out.printf("Input: ");
//                    int read = System.in.read(b);
//                    System.out.println(read);
//                    if (b[0]=='c') waiting =false;
//                }

                System.out.println("R: Initialize Spark Context...");
                long before = System.nanoTime();
                context = getJavaSparkContext(ReadFiles.class.getSimpleName());
                long after = System.nanoTime();
                System.out.println("Done! ["+(after-before)/1000000000+"sec] ==> " + context.getConf().toDebugString());

                JavaRDD<String> stringJavaRDD = context.textFile("hdfs://myd-vm22661.hpswlabs.adapps.hp.com.hpswlabs.adapps.hp.com:8020/user/hadoop/ebooks/gutenberg_input.txt");
                stringJavaRDD.collect().forEach(System.out::print);


//                loopthemall(context);


            } catch (Exception e) {
                if (context != null) {
                    context.stop();
                }
                e.printStackTrace();
            }
            finally {
                finished = true;
            }
        }

        protected void loopthemall(JavaSparkContext context) throws InterruptedException {
            for (int i = 0; i < 50000 ;  i++) {

                try {
                    FileUtils.deleteDirectory(new File(TEXT_OUTPUT_FOLDER));
                } catch (IOException e) {
                    e.printStackTrace();
                }


                JavaRDD<String> file     = context.textFile(INPUT_FILE);
                JavaRDD<String>    filtered = file.filter(F_FILTER_FILE);
                JavaRDD<StocksVal> stocks   = filtered.map(F_READ_2_OBJECTS);
                JavaRDD<StocksVal> distinct = stocks.distinct(4);



                System.out.println("R: " + i + ":\tfile_count="+file.count() + " | filtered_count="+filtered.count() + " | stocks_count="+stocks.count() + " | distinct_count="+distinct.count());
                System.out.println(Thread.currentThread().getName());

                Thread.sleep(SLEEP_TIME);
            }
        }
    }

    private static class Ex implements Runnable {

        @Override
        public void run() {
            System.out.println("Ex: Starting...");
            while (waiting) {
                try {
                    for (int i = 0; i < 10000; i++) {
                        list.add(new Random(i).nextInt(500));
                    }
                    if (list.size()>50000) {
                        throw new IndexOutOfBoundsException("Clearing the list");
                    }
                    Thread.sleep(1000);
                } catch (IndexOutOfBoundsException e) {
                    list.clear();
                    System.out.println("Ex: " + e.getMessage());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            System.out.println("Ex: Finished!");
        }
    }

//        stocks.saveAsTextFile(TEXT_OUTPUT_FOLDER);
//        stocks.saveAsObjectFile(WordCount.INPUT_FILE+".saveAsObjectFile.utxt");

}
