package jaceklaskowski.gitbooks.io;

import golan.hello.spark.core.AbsSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.StreamSupport;

/**
 * Created by golaniz on 25/01/2016.
 *
 * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/exercises/spark-exercise-pairrddfunctions-oneliners.html
 */
public class TestPairRDDFunctions extends AbsSpark {

    public static final Function<String, String[]> F_SPLIT_TRIM = s -> Arrays.asList(s.split(",")).stream().map(String::trim).toArray(String[]::new);

    public static void main(String[] args) {

        //The file contains a list of team members in our group
        //For each line we have these values: {TEAM, F_NAME, L_NAME}

        JavaSparkContext context = getJavaSparkContext("TestPairRDDFunctions");
        JavaRDD<String> file = context.textFile(INPUT_FOLDER + "\\PairRDDFunctions.txt").cache();

        JavaRDD<String[]> lines = file.map(F_SPLIT_TRIM);
        JavaPairRDD<String, Iterable<String[]>> groupByTeam = lines.groupBy(x -> x[0]);
        JavaRDD<Tuple2<String, String[]>> membersPerTeam = groupByTeam.map(t -> new Tuple2<>(t._1(), StreamSupport.stream(t._2().spliterator(), false).map(s -> s[1] + " " + s[2]).toArray(String[]::new)));
        membersPerTeam = membersPerTeam.sortBy(Tuple2::_1, true, 1);
        System.out.println("####################################################################################");
        membersPerTeam.foreach(t -> System.out.println("T["+t._1()+"]: " + Arrays.toString(t._2())));
        System.out.println("####################################################################################");

    }


}
