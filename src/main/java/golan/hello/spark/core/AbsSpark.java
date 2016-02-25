package golan.hello.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by golaniz on 25/01/2016.
 */
public class AbsSpark {


    public static final String INPUT_FOLDER = "C:\\Users\\golaniz\\Desktop\\Misc\\rdd";
    public static final String INPUT_FILE = INPUT_FOLDER + "\\StocksByDate.utxt";
    public static final String TEXT_OUTPUT_FOLDER = INPUT_FOLDER + "\\StocksByDateOutput";

    public static final Function<String, Boolean> F_FILTER_FILE =  StocksVal::isFirstDayOfMonth;

    public static final Function<String, StocksVal> F_READ_2_OBJECTS = StocksVal::new;

    protected static JavaSparkContext getJavaSparkContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local[*]");
//        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("spark://myd-vm22661.hpswlabs.adapps.hp.com:7077");
        return new JavaSparkContext(sparkConf);
    }
}
