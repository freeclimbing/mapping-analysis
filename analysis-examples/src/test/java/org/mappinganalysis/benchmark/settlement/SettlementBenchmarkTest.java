package org.mappinganalysis.benchmark.settlement;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

/**
 * No test for benchmarks
 */
public class SettlementBenchmarkTest {
  private static ExecutionEnvironment env;

  @Test
  public void testMain() throws Exception {
    SettlementBenchmark benchmark = new SettlementBenchmark();
  }
}