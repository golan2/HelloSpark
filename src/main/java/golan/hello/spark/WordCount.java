
package golan.hello.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
public class WordCount {

  public static final String INPUT_FILE = "C:\\Users\\golaniz\\Desktop\\Misc\\rdd\\StocksByDate.utxt";

//  private static final FlatMapFunction<String, String> D_WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
//    @Override
//    public Iterable<String> call(String s) throws Exception {
//      System.out.println("D_WORDS_EXTRACTOR("+s+")");
//      List<String> result = Arrays.asList(s.split(","));
//      return result;
//    }
//  };

  public static final Function<String, Boolean> D_FILTER_FILE = new Function<String, Boolean>() {
    @Override
    public Boolean call(String s) throws Exception {
      boolean firstDayOfMonth = isFirstDayOfMonth(s);
      if (firstDayOfMonth) System.out.println("D_FILTER_FILE(" + s + ")");
      return firstDayOfMonth;
    }

    private boolean isFirstDayOfMonth(String s) {
      StocksVal stocksVal = new StocksVal(s);
      return (1==stocksVal.getDay());
//      String date = stocksVal.date;
//      if (StocksVal.N_A.equals(date)) return false;
//      String[] cols = date.split("/");
//      if (cols.length<1) return false;
//      String day = cols[0].trim();
//      return "01".equals(day);
    }
  };

  public static final Function<String, StocksVal> D_READ_2_OBJECTS = new Function<String, StocksVal>() {
    @Override
    public StocksVal call(String s) throws Exception {
      StocksVal result = new StocksVal(s);
      System.out.println("D_READ_2_OBJECTS("+result.id+")");
      return result;
    }
  };


  private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
          (FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" "));

  private static final PairFunction<String, String, Integer> WORDS_MAPPER =
          (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1);
  private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
          (Function2<Integer, Integer, Integer>) (a, b) -> a + b;


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


//
//    JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
//    System.out.println("pairs count: " + pairs.count());
//    JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
//    System.out.println("counter.countByKey: " + counter.countByKey());



//    counter.saveAsTextFile(args[1]);


  }

  private static void run() {
    System.out.println("-------------------------------------- run --------------------------------------");
    System.out.println("Before...");
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("org.sparkexample.WordCount");
    sparkConf.setMaster("local");
    System.out.println("sparkConf: " + sparkConf);
    JavaSparkContext context = new JavaSparkContext(sparkConf);
    System.out.println("context: " + context);

    JavaRDD<String> file = context.textFile(INPUT_FILE);
    System.out.println("D_FILTER_FILE...");
    file = file.filter(D_FILTER_FILE);
    System.out.println("D_READ_2_OBJECTS...");
    JavaRDD<StocksVal> stocks = file.map(D_READ_2_OBJECTS);
    System.out.println("foreach...");
    stocks.foreach(stocksVal -> System.out.println(stocksVal.date+"#v1+v2="+(stocksVal.val1+stocksVal.val2)/2));
  }


}
