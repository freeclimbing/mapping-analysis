package org.mappinganalysis.benchmark.settlement;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Extra program to run the single type benchmark containing only settlements from OAEI.
 */
public class SettlementBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  public static void main(String[] args) throws Exception {
    // read input
    Graph<Long, ObjectMap, NullValue> inGraph = Utils.readFromJSONFile(
        Constants.LL_MODE.concat(Constants.INPUT_GRAPH),
        ObjectMap.class,
        NullValue.class,
        env);

    ExampleOutput out = new ExampleOutput(env);
    Constants.IGNORE_MISSING_PROPERTIES = true;


    // preprocessing
    Graph<Long, ObjectMap, ObjectMap> graph
        = Preprocessing.execute(inGraph, Constants.VERBOSITY, out, env);
    // decomposition
    // merge
  }

  @Override
  public String getDescription() {
    return SettlementBenchmark.class.getName();
  }
}
