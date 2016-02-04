package jaceklaskowski.gitbooks.io;

import golan.hello.spark.AbsSpark;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by golaniz on 25/01/2016.
 */
public class TakeMultipleJobs extends AbsSpark {

    public static final Integer[] VALUES = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};

    public static void main(String[] args) {
        JavaSparkContext sc = getJavaSparkContext("TakeMultipleJobs");
        JavaRDD<Integer> values = sc.parallelize((List<Integer>) Arrays.asList(VALUES), 16);
        for (Partition partition : values.partitions()) {
            System.out.println(partition.toString());
        }

    }
}
