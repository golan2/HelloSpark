package golan.hello.spark.streaming;

import golan.hello.spark.core.CmdOpts;
import golan.hello.spark.core.Utils;
import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by golaniz on 11/02/2016.
 */
public class KafkaStreamCountMessages {

    public static final String APP_NAME = KafkaStreamCountMessages.class.getSimpleName();


    //
    public static void main(String[] args) throws IOException, ParseException {
        KafkaConsumerHelper helper = new KafkaConsumerHelper(args);
        JavaStreamingContext jssc = null;
        try {
            jssc = helper.createJavaStreamingContext(APP_NAME);
            Utils.consolog("jssc=[" + jssc + "]");

            JavaPairReceiverInputDStream<String, String> messages = helper.createReceiverStream(jssc);
            Utils.consolog("messages=[" + messages + "]");

            countMessagesPerProducer(messages);

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

    /**
     * We get pairs of <key,message> the key is a simple UUID
     * The message format is <producer-id>_<msg-value>
     * Ignore the keys and work on the messages
     * Split the messages according to "_"
     * Create a new set of pairs with <producer-id,msg-value>
     */
    protected static void countMessagesPerProducer(JavaPairReceiverInputDStream<String, String> messages) {
        messages.foreachRDD(new VoidFunction2<JavaPairRDD<String, String>, Time>() {
            @Override
            public void call(JavaPairRDD<String, String> pairs, Time time) throws Exception {
                if (pairs.count()>0) {
                    Utils.consolog("outer ==> pairs.count=["+pairs.count()+"] time=["+time+"]");


                    JavaPairRDD<String, String> messageAndProducerId = pairs.<String,String>mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                        @Override
                        public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
                            String[] split = t._2().split("_");
                            String producerId = split[0];
                            String msgValue = split[1].trim();
                            return new Tuple2<>(producerId, msgValue);                        }
                    });
                    Map<String, Object> counters = messageAndProducerId.countByKey();
                    if (!counters.isEmpty()) {
                        StringBuilder buf = new StringBuilder();
                        buf.append("{ ");
                        ArrayList<String> sortedKeys = new ArrayList<>(counters.keySet());
                        sortedKeys.sort((o1, o2) -> Integer.valueOf(o1).compareTo(Integer.valueOf(o2)));
                        for (String key : sortedKeys) {
                            Object value = counters.get(key);
                            buf.append("(" + key + "," + value + "),");
                        }
                        buf.append(" }");
                        Utils.consolog("inner ==> " + buf.toString());
                    }


                }
            }
        });

    }

}
