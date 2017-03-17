package org.mappinganalysis.benchmark.settlement;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Extra program to run the single type benchmark containing only settlements from OAEI.
 */
public class SettlementBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  public static final String PREPROCESSING = "settlement-preprocessing";
  public static final String DECOMPOSITION = "settlement-decomposition-representatives";
  public static final String MERGE = "settlement-merged-clusters";
  public static final String PRE_JOB = "Settlement Preprocessing";
  public static final String DEC_JOB = "Settlement Decomposition + Representatives";
  public static final String MER_JOB = "Settlement Merge";

  /**
   * Main class for Settlement benchmark
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
        args.length == 1, "args[0]: input dir");
    Constants.SOURCE_COUNT = 4;
    Constants.INPUT_DIR = args[0];

    // preprocessing
    Graph<Long, ObjectMap, NullValue> preprocGraph =
        Utils.readFromJSONFile(
            "input",
            ObjectMap.class,
            NullValue.class,
            env);

    Utils.writeGraphToJSONFile(
        preprocGraph.run(new DefaultPreprocessing(true, env)),
        PREPROCESSING);
    env.execute(PRE_JOB);

//    // decomposition with representative creation
//    Graph<Long, ObjectMap, ObjectMap> decompGraph =
//        Utils.readFromJSONFile(PREPROCESSING, env)
//            .run(new TypeGroupBy(env)) // not needed? TODO
//            .run(new SimSort(true, env));
//
//    Utils.writeVerticesToJSONFile(
//        decompGraph.getVertices()
//            .runOperation(new RepresentativeCreator()),
//        DECOMPOSITION);
//    env.execute(DEC_JOB);
//
//    // merge
//    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
//        Utils.readVerticesFromJSONFile(Constants.SIMSORT_GRAPH, env, false)
//            .runOperation(new MergeInitialization())
//            .runOperation(new MergeExecution(Constants.SOURCE_COUNT));
//
//    Utils.writeVerticesToJSONFile(mergedVertices, MERGE);
//    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return SettlementBenchmark.class.getName();
  }
}
