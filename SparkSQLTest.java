package com.mercury.diagnostics.server.persistence.queryAPI;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SQLContext.implicits$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Char;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
* Created by kedemg on 08/12/15.
*/
public class SparkSQLTest {
//  private static String = "A B C D E F G H I J  K  L  M  N  O  P  Q  R  S  T  U  V  W  X"
//                          "0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
  public static void main(String[] args) {
    JavaSparkContext sc = getJavaSparkContext();
    selectSample1(sc);
//    reduceFileSize();
  }

  private static void reduceFileSize() {
    try (BufferedReader fis = new BufferedReader(new FileReader("c:\\Temp\\pagecounts-2015-11-18_full"));
         BufferedWriter fos = new BufferedWriter(new FileWriter("c:\\Temp\\pagecounts-2015-11-18"));) {
      int i=0;
      int maxLines = 10000000;
      while (i < maxLines) {
        String s = fis.readLine();
        fos.write(s, 0, s.length());
        fos.newLine();
        i++;
      }
//      fis.close();
//      fos.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static JavaSparkContext getJavaSparkContext() {
    SparkConf conf = new SparkConf().setAppName("com.mercury.diagnostics.server.SparkSQLTest").setMaster("local");//.set("");
    return new JavaSparkContext(conf);
  }
  public static void selectSample1(JavaSparkContext sc) {
    StopWatch sw = new StopWatch();
    sw.start();
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    JavaRDD wikiStats = sc.textFile("c:\\Temp\\pagecounts-2015-11-18.txt");
    JavaRDD rows = wikiStats.filter(row -> !row.toString().startsWith("#")).map(record -> ((String) record).split(" "));
    JavaRDD<WikiPage> records = rows.map(splitted -> WikiPage.fromStrings((String[]) splitted));
    // The schema is encoded in a string


    DataFrame wikiPages = sqlContext.createDataFrame(records, WikiPage.class); //WikiPage.class);
    wikiPages.registerTempTable("pages");
    wikiPages.cache();
    Row first = wikiPages.first();
    System.out.println(first);

    sw.suspend();
    System.out.println("Split Time: " + sw.getTime());
    sw.reset();
    //SELECT row from table ORDER BY id DESC LIMIT 1
    String sqlText = "SELECT * FROM pages ORDER BY totalCount DESC LIMIT 1";
    executeQuery(sqlContext, sqlText);
    executeQuery(sqlContext, sqlText);

    sqlText = "SELECT * FROM pages WHERE page='Kategorie:Alexanderreich'";
    executeQuery(sqlContext, sqlText);
  }

  private static void executeQuery(SQLContext sqlContext, String sqlText) {
    StopWatch sw = new StopWatch();
    sw.start();
    DataFrame result = sqlContext.sql(sqlText);
    result.show();
    sw.stop();
    System.out.println("Query: " + sqlText + "\nExecution Time: " + sw.getTime());
  }
}
