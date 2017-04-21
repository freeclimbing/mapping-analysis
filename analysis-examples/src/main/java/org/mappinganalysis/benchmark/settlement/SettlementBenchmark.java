package org.mappinganalysis.benchmark.settlement;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
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

  public static String INPUT_PATH;

  /**
   * Main class for Settlement benchmark
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 1, "args[0]: input dir");
    Constants.SOURCE_COUNT = 4;
    INPUT_PATH = args[0];
    Double minSimSortSim = 0.7;

    /**
     * preprocessing
     */
    Graph<Long, ObjectMap, NullValue> preprocGraph =
        new JSONDataSource(INPUT_PATH, env)
            .getGraph(ObjectMap.class, NullValue.class);

    new JSONDataSink(INPUT_PATH, PREPROCESSING)
        .writeGraph(preprocGraph.run(new DefaultPreprocessing(true, env)));
    env.execute(PRE_JOB);

    /**
     * decomposition with representative creation
     */
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(INPUT_PATH, PREPROCESSING, env)
        .getGraph()
//        .run(new TypeGroupBy(env)) // not needed? TODO
        .run(new SimSort(minSimSortSim, env))
        .getVertices()
        .runOperation(new RepresentativeCreator());

    new JSONDataSink(INPUT_PATH, DECOMPOSITION)
        .writeVertices(vertices);
    env.execute(DEC_JOB);

    /**
     * merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(INPUT_PATH, DECOMPOSITION, env)
            .getVertices()
            .runOperation(new MergeInitialization())
            .runOperation(new MergeExecution(Constants.SOURCE_COUNT));

    new JSONDataSink(INPUT_PATH, MERGE)
        .writeVertices(mergedVertices);
    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return SettlementBenchmark.class.getName();
  }
}
