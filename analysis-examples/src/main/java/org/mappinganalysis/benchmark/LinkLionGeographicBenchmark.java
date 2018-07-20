package org.mappinganalysis.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * LL Benchmark, old.
 */
public class LinkLionGeographicBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String PREPROCESSING = "geographic-preprocessing";
  private static final String DECOMPOSITION = "geographic-decomposition-representatives";
  private static final String MERGE = "geographic-merged-clusters";
  private static final String PRE_JOB = "Geographic Preprocessing";
  private static final String DEC_JOB = "Geographic Decomposition + Representatives";
  private static final String MER_JOB = "Geographic Merge";

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2,
        "args[0]: input dir, " +
            "args[1]: min SimSort similarity (e.g., 0.7)");

    final String inputPath = args[0];
    IncrementalConfig config = new IncrementalConfig(DataDomain.GEOGRAPHY, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(Doubles.tryParse(args[1]));

    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(
        inputPath,
        Constants.INPUT_GRAPH,
        env)
        .getGraph(ObjectMap.class, NullValue.class)
        .run(new DefaultPreprocessing(config));

    new JSONDataSink(inputPath, PREPROCESSING)
        .writeGraph(graph);
    env.execute(PRE_JOB);

    /*
      Decomposition
     */
    DataSet<Vertex<Long, ObjectMap>> representatives =
        new JSONDataSource(inputPath, PREPROCESSING, env)
            .getGraph()
            .run(new TypeGroupBy(env))
            .run(new SimSort(config))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.GEOGRAPHY));

    new JSONDataSink(inputPath, DECOMPOSITION)
        .writeVertices(representatives);
    env.execute(DEC_JOB);

    /*
      Merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(inputPath, DECOMPOSITION, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY))
            .runOperation(new MergeExecution(DataDomain.GEOGRAPHY,
                config.getMetric(),
                0.5,
                5,
                env));

    new JSONDataSink(inputPath, MERGE)
        .writeVertices(mergedVertices);
    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return LinkLionGeographicBenchmark.class.getName();
  }
}
