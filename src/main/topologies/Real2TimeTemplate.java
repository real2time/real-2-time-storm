package topologies;

import java.util.UUID;

import spout.*;
import storm.kafka.*;
import bolt.*;
import util.*;

import org.apache.log4j.Logger;

import util.StormRunner;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class Real2TimeTemplate {

    private static final Logger LOG = Logger.getLogger(Real2TimeTemplate.class);
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
    private static final int TOP_N = 5;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public Real2TimeTemplate(String topologyName) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {

        //[replace_here]

    }

    public void runLocally() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public void runRemotely() throws Exception {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    }

    public static void main(String[] args) throws Exception {
        String topologyName = "Real2TimeTemplate";
        if (args.length >= 1) {
          topologyName = args[0];
        }

        LOG.info("Topology name: " + topologyName);
        Real2TimeTemplate rtw = new Real2TimeTemplate(topologyName);

        rtw.runRemotely();
        //rtw.runLocally();
    }

}
