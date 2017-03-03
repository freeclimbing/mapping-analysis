package org.mappinganalysis.benchmark.settlement;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Extra program to run the single type benchmark containing only settlements from OAEI.
 */
public class SettlementBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  public static final String PREPROCESSING = "settlement-preprocessing";


  public static void main(String[] args) throws Exception {
    // read input
    Graph<Long, ObjectMap, NullValue> inGraph = Utils.readFromJSONFile(
        Constants.LL_MODE.concat(Constants.INPUT_GRAPH),
        ObjectMap.class,
        NullValue.class,
        env);

    // preprocessing
    Graph<Long, ObjectMap, ObjectMap> graph = inGraph
        .run(new DefaultPreprocessing(true, env));

    Utils.writeGraphToJSONFile(graph, Constants.LL_MODE.concat(PREPROCESSING));
    env.execute("Settlement Preprocessing");

//    graph = Utils.readFromJSONFile(
//        Constants.LL_MODE.concat(PREPROCESSING),
//        env)
//        .run(new TypeGroupBy(env)) // not needed? TODO
//        .run(new SimSort(env));
//    // decomposition
//    // merge
  }

  @Override
  public String getDescription() {
    return SettlementBenchmark.class.getName();
  }
}
