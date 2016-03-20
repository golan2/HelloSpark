package golan.hello.spark.core;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by golaniz on 20/01/2016.
 */
public class ReadFiles extends AbsSpark {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================== BEGIN ====================================");

        JavaRDD<String>    file     = getJavaSparkContext("StocksByDate").textFile(Utils.findFileInClasspath(STOCKS_FILE));
        JavaRDD<String>    filtered = file.filter(StocksVal::isFirstDayOfMonth);
        JavaRDD<StocksVal> stocks   = filtered.map(StocksVal::new);
        JavaRDD<StocksVal> distinct = stocks.distinct(4);
        System.out.println("Processing completed! \tfile_count="+file.count() + " | filtered_count="+filtered.count() + " | stocks_count="+stocks.count() + " | distinct_count="+distinct.count());
        System.out.println(Thread.currentThread().getName());


        System.out.println("========================== E N D ====================================");
    }


}
