package org.mappinganalysis.benchmark.settlement;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.corruption.DataCorruption;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Extra program to run the single type benchmark containing only settlements from OAEI.
 */
public class SettlementBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String PREPROCESSING = "settlement-preprocessing";
  private static final String CORRUPTED = "settlement-corrupted";
  private static final String DECOMPOSITION = "settlement-decomposition-representatives";
  private static final String MERGE = "settlement-merged-clusters";
  private static final String PRE_JOB = "Settlement Preprocessing";
  private static final String DEC_JOB = "Settlement Decomposition + Representatives";
  private static final String MER_JOB = "Settlement Merge";

  private static String INPUT_PATH;

  /**
   * Main class for Settlement benchmark
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2, "args[0]: input dir, args[1]: isCorrupted");
    Constants.SOURCE_COUNT = 4;
    boolean isCorrupted = args[1].equals("isCorrupted");
    INPUT_PATH = args[0];
    Double minSimSortSim = 0.7;

    /*
      preprocessing
     */
    Graph<Long, ObjectMap, NullValue> preprocGraph =
        new JSONDataSource(INPUT_PATH, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .run(new DataCorruption(env));

    if (isCorrupted) {
      // additional corrupt
      Graph<Long, ObjectMap, NullValue> tmpGraph = preprocGraph
          .run(new ConnectedComponentIdAdder<>(env));

      DataSet<Edge<Long, NullValue>> distinctEdges = tmpGraph
          .getVertices()
          .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector()));

      new JSONDataSink(INPUT_PATH, CORRUPTED)
          .writeGraph(Graph.fromDataSet(tmpGraph.getVertices(), distinctEdges, env));
    }

    new JSONDataSink(INPUT_PATH, PREPROCESSING)
        .writeGraph(preprocGraph
            .run(new DefaultPreprocessing(true, env)));
    env.execute(PRE_JOB);

    /*
      decomposition with representative creation
     */
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(INPUT_PATH, PREPROCESSING, env)
        .getGraph()
//        .run(new TypeGroupBy(env)) // not needed? TODO
        .run(new SimSort(DataDomain.GEOGRAPHY, minSimSortSim, env))
        .getVertices()
        .runOperation(new RepresentativeCreator(DataDomain.GEOGRAPHY));

    new JSONDataSink(INPUT_PATH, DECOMPOSITION)
        .writeVertices(vertices);
    env.execute(DEC_JOB);

    /*
      merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(INPUT_PATH, DECOMPOSITION, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY))
            .runOperation(new MergeExecution(DataDomain.GEOGRAPHY, Constants.SOURCE_COUNT, env));

    new JSONDataSink(INPUT_PATH, MERGE)
        .writeVertices(mergedVertices);
    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return SettlementBenchmark.class.getName();
  }
}
