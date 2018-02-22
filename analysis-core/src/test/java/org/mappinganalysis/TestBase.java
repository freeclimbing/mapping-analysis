package org.mappinganalysis;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.mappinganalysis.util.Constants;

/**
 * Base methods testing
 */
public class TestBase {
  public static void setupConstants() {
    Constants.INPUT_PATH = "linklion";
    Constants.SOURCE_COUNT = 5;
  }

  public static ExecutionEnvironment setupLocalEnvironment() {
    Configuration conf = new Configuration();
    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 16384);
    ExecutionEnvironment env = new LocalEnvironment(conf);
    env.setParallelism(Runtime.getRuntime().availableProcessors());
    env.getConfig().disableSysoutLogging();

    return env;
  }
}
