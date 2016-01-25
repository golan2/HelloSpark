
package golan.hello.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 * <B>Copyright:</B>   HP Software IL
 * <B>Owner:</B>       <a href="mailto:izik.golan@hp.com">Izik Golan</a>
 * <B>Creation:</B>    12/11/2015 09:05
 * <B>Since:</B>       BSM 9.21
 * <B>Description:</B>
 *
 * </pre>
 */
public class WordCount extends AbsSpark {

    private static final FlatMapFunction<String, String>       WORDS_EXTRACTOR = (FlatMapFunction<String, String>      ) s -> Arrays.asList(s.split(" "));
    private static final PairFunction<String, String, Integer> WORDS_MAPPER    = (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1);
    private static final Function2<Integer, Integer, Integer>  WORDS_REDUCER   = (Function2<Integer, Integer, Integer> ) (a, b) -> a + b;


  public static void main(String[] args) {
    boolean doAgain = true;
    while (true) {
      try {
        if (doAgain) {
          run();
          doAgain = false;
        }
        Thread.sleep(5000);
      } catch (InterruptedException ignore) {
      }
    }

  }

  private static void run() {
    System.out.println("-------------------------------------- run --------------------------------------");
    System.out.println("Before...");
    JavaSparkContext context = getJavaSparkContext("WordCount");
    System.out.println("context: " + context);

    JavaRDD<String> file = context.textFile(AbsSpark.INPUT_FILE);
    System.out.println("F_FILTER_FILE...");
    file = file.filter(AbsSpark.F_FILTER_FILE);
    System.out.println("F_READ_2_OBJECTS...");
    JavaRDD<StocksVal> stocks = file.map(AbsSpark.F_READ_2_OBJECTS);
    System.out.println("foreach...");
    stocks.foreach(stocksVal -> System.out.println(stocksVal.date+"#v1+v2="+(stocksVal.val1+stocksVal.val2)/2));
  }


}
