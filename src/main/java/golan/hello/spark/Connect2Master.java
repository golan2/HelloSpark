package golan.hello.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by golaniz on 21/01/2016.
 */
public class Connect2Master {

    public static void main(String[] args) {
        //In order for the following line to work you need a spark master.
        //Go to    C:\classpath\spark\spark-1.5.2-bin-hadoop2.6\bin
        //Run      spark-class.cmd org.apache.spark.deploy.master.Master

        SparkConf sparkConf = new SparkConf().setAppName("Connect2Master").setMaster("spark://127.0.0.1:7077").set("spark.ui.port","4041").set("spark.cores.max","4");
        System.out.println("~~sparkConf=" + sparkConf);
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        System.out.println("~~context=" + context);
        JavaRDD<String> file = context.textFile(AbsSpark.INPUT_FILE);
        System.out.println("~~file_count=" + file.count());
    }
}
