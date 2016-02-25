package golan.hello.spark.core;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by golaniz on 17/02/2016.
 */
public class CmdOpts {
    public static final int    NUM_THREADS    = 1;
    public static final String TOPIC_NAME     = "test1";
    public static final String ZK_QUORUM      = "myd-vm23458.hpswlabs.adapps.hp.com:2181";
    public static final String CONSUMER_GROUP = "group_1";
    public static final String SPARK_MASTER   = "local[*]";
    public static final String NO_FILE        = "{console}";

    final int         numberOfThreads;
    final Set<String> topicNames;
    final String      zkQuorum;
    final String      consumerGroup;
    final String      sparkMaster;
    final String      outputFile;

    @SuppressWarnings("AccessStaticViaInstance")
    public CmdOpts(String[] args) throws ParseException {
        Options o = new Options();
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("numberOfThreads"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("topicNames"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("zkQuorum"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("consumerGroup"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sparkMaster"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("outputFile"));
        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(o, args);

        this.numberOfThreads = Integer.parseInt(line.getOptionValue("numberOfThreads", String.valueOf(NUM_THREADS)));
        this.topicNames = new HashSet<>(Arrays.asList(line.getOptionValue("topicNames", TOPIC_NAME).split(",")).stream().map(String::trim).collect(Collectors.toList()));
        this.zkQuorum = line.getOptionValue("zkQuorum", ZK_QUORUM);
        this.consumerGroup = line.getOptionValue("consumerGroup", CONSUMER_GROUP);
        this.sparkMaster = line.getOptionValue("sparkMaster", SPARK_MASTER);
        this.outputFile = line.getOptionValue("outputFile", NO_FILE);
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }
}
