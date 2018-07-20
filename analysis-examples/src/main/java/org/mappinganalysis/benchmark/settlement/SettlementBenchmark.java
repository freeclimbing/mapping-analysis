package org.mappinganalysis.benchmark.settlement;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;
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

  /**
   * Main class for Settlement benchmark
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 4,
        "args[0]: input dir, args[1]: isCorrupted/not, args[2]: metric, args[3]: csimq/normal");
    int sourceCount = 4;
    boolean isCorrupted = args[1].equals("isCorrupted");
    String inputPath = args[0];

    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.7);
    config.setMetric(args[2]);

    /*
      preprocessing
     */
    Graph<Long, ObjectMap, NullValue> preprocGraph =
        new JSONDataSource(inputPath, env)
            .getGraph(ObjectMap.class, NullValue.class);
//            .run(new DataCorruption(env));

    if (isCorrupted) {
      // additional corrupt
      Graph<Long, ObjectMap, NullValue> tmpGraph = preprocGraph
          .run(new ConnectedComponentIdAdder<>(env));

      DataSet<Edge<Long, NullValue>> distinctEdges = tmpGraph
          .getVertices()
          .runOperation(new EdgeComputationOnVerticesForKeySelector(new CcIdKeySelector()));

      new JSONDataSink(inputPath, CORRUPTED)
          .writeGraph(Graph.fromDataSet(tmpGraph.getVertices(), distinctEdges, env));
    }

    new JSONDataSink(inputPath, PREPROCESSING)
        .writeGraph(preprocGraph
            .run(new DefaultPreprocessing(config)));
    env.execute(PRE_JOB);

    /*
      decomposition with representative creation
     */
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(inputPath, PREPROCESSING, env)
        .getGraph()
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.GEOGRAPHY));

    new JSONDataSink(inputPath, DECOMPOSITION)
        .writeVertices(vertices);
    env.execute(DEC_JOB);

    /*
      merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(inputPath, DECOMPOSITION, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY))
            .runOperation(new MergeExecution(
                DataDomain.GEOGRAPHY,
                config.getMetric(),
                0.5,
                sourceCount,
                env));

    new JSONDataSink(inputPath, MERGE)
        .writeVertices(mergedVertices);
    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return SettlementBenchmark.class.getName();
  }
}
