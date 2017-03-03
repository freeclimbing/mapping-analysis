package org.mappinganalysis.benchmark.settlement;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class SettlementBenchmarkTest {
  private static ExecutionEnvironment env;

  @Test
  public void testMain() throws Exception {
    setupLocalEnvironment();

    SettlementBenchmark benchmark = new SettlementBenchmark();
  }

  public static ExecutionEnvironment setupLocalEnvironment() {
    Configuration conf = new Configuration();
    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 16384);
    env = new LocalEnvironment(conf);
    env.setParallelism(Runtime.getRuntime().availableProcessors());
    env.getConfig().disableSysoutLogging();

    return env;
  }
}