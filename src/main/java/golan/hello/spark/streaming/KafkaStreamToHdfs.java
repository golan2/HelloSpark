package golan.hello.spark.streaming;

import golan.hello.spark.core.Utils;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by golaniz on 17/02/2016.
 */
public class KafkaStreamToHdfs {
    public static final String APP_NAME = KafkaStreamToHdfs.class.getSimpleName();

    public static void main(String[] args) throws IOException, ParseException {
        KafkaConsumerHelper helper = new KafkaConsumerHelper(args);
        JavaStreamingContext jssc = null;
        try {
            jssc = helper.createJavaStreamingContext(APP_NAME);
            JavaPairReceiverInputDStream<String, String> rs = helper.createReceiverStream(jssc);

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("dfs.replication", "1");
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            rs.saveAsNewAPIHadoopFiles("prefix", "txt", Integer.class, Integer.class, TextOutputFormat.class, conf);

//            rs.saveAsHadoopFiles("qwe_prefix_", "_qwe_suffix", );
//            JavaDStream<Text> map = rs.map(t -> new Text(t._2()));


//            rs.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
//                @Override
//                public void call(JavaPairRDD<String, String> pair) throws Exception {
//                    pair.saveAsHadoopFile();
//                }
//            });

            Utils.consolog("start...awaitTermination...    (type 'Q' to finish)");
            helper.startAndWait(jssc);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jssc != null) {
                Utils.consolog("stopping...closing...");
                helper.stopAndClose(jssc);

                System.out.println("~~~~~~~~~~~~~~~~~~~~~~kafkaStream.saveAsHadoopFiles");

            }
            Utils.consolog("~~ DONE ~~");
        }
    }

    public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
        public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
            return new Tuple2(new Text(record._1), new IntWritable(record._2));
        }
    }
}
