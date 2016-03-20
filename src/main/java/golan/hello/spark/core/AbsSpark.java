package golan.hello.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by golaniz on 25/01/2016.
 */
public class AbsSpark {
    public static final String STOCKS_FILE           = "StocksByDate.utxt";
    public static final String PAIR_RDD_FUNCTIONS_TXT = "PairRDDFunctions.txt";

    protected static JavaSparkContext getJavaSparkContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local[*]");
//        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("spark://myd-vm22661.hpswlabs.adapps.hp.com:7077");

        return new JavaSparkContext(sparkConf);
    }
}
