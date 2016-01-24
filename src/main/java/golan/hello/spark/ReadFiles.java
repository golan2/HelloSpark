package golan.hello.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

/**
 * Created by golaniz on 20/01/2016.
 */
public class ReadFiles {

    public static final String TEXT_OUTPUT_FOLDER = WordCount.INPUT_FILE + ".saveAsTextFile.utxt";

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = context.textFile(WordCount.INPUT_FILE);
        JavaRDD<String> filtered = file.filter(WordCount.D_FILTER_FILE);
        JavaRDD<StocksVal> stocks = filtered.map(WordCount.D_READ_2_OBJECTS);
        try {
            FileUtils.deleteDirectory(new File(TEXT_OUTPUT_FOLDER));
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaRDD<StocksVal> distinct = stocks.distinct(4);

        System.out.println("file_count="+file.count());
        System.out.println("filtered_count="+filtered.count());
        System.out.println("stocks_count="+stocks.count());
        System.out.println("distinct_count="+distinct.count());

//        stocks.saveAsTextFile(TEXT_OUTPUT_FOLDER);

        System.out.println("Thread.sleep =============================");

        Thread.sleep(35000);

        System.out.println("The End =============================");

//        stocks.saveAsObjectFile(WordCount.INPUT_FILE+".saveAsObjectFile.utxt");

//        stocks.foreach((VoidFunction<StocksVal>) new VoidFunction<StocksVal>() {
//            @Override
//            public void call(StocksVal stocksVal) throws Exception {
//                FileOutputStream fos = new FileOutputStream(WordCount.INPUT_FILE+".foreach.utxt", true);
//                ObjectOutputStream oos = new ObjectOutputStream(fos);
//                oos.writeObject(stocksVal.toString());
//                fos.close();
//            }
//        });


        System.out.println("context.stop =============================");
        context.stop();

    }
}
