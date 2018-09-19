package org.mappinganalysis.benchmark.nc;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;

public class StaticNCBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private static final String DECOMPOSITION = "nc-decomposition-representatives";
  private static final String MERGE = "nc-merged-clusters";
  private static final String DEC_JOB = "NC Decomposition + Representatives";
  private static final String MER_JOB = "NC Merge";

  /**
   * Main class for NC benchmark.
   * DINA 2018 paper comparison Split+Merge numbers static approach.
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 3,
        "args[0]: input dir, " +
            "args[1]: blocking length (2, 4, default: 6)" +
            "args[2]: mergeThreshold (minResultSim)");
    final String inputPath = args[0];
    final int blockingLength = (Ints.tryParse(args[1]) == null)
        ? 6 : Ints.tryParse(args[1]);

    final Double preThreshold = Doubles.tryParse(args[2]);
    final double mergeThreshold = (preThreshold == null)
        ? 0.8 : preThreshold;

    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.5);
    config.setBlockingLength(blockingLength);
    config.setMinResultSimilarity(mergeThreshold);
    config.setExistingSourcesCount(5);

    // Read input graph
    LogicalGraph logicalGraph = Utils
        .getGradoopGraph(inputPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    // Decomposition + Representative
    DataSet<Vertex<Long, ObjectMap>> representatives = graph
        .run(new DefaultPreprocessing(config))
        .run(new TypeGroupBy(env))
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    new JSONDataSink(inputPath, DECOMPOSITION)
        .writeVertices(representatives);
    env.execute(DEC_JOB);

    DataSet<Vertex<Long, ObjectMap>> statsVertices = new JSONDataSource(
        inputPath, DECOMPOSITION, env)
        .getVertices();

    QualityUtils.printNcQuality(statsVertices,
        config,
        inputPath,
        "cluster",
        "5");

    // Read graph from disk and merge
    representatives = new JSONDataSource(
        inputPath, DECOMPOSITION, env)
        .getVertices();

    DataSet<Vertex<Long, ObjectMap>> merged = representatives
        .runOperation(new MergeInitialization(DataDomain.NC))
        .runOperation(new MergeExecution(config));

    new JSONDataSink(inputPath, MERGE)
        .writeVertices(merged);
    env.execute(MER_JOB);

    DataSet<Vertex<Long, ObjectMap>> clusters = new JSONDataSource(inputPath, MERGE, env)
        .getGraph()
        .getVertices();

    QualityUtils.printNcQuality(clusters,
        config,
        inputPath,
        "cluster",
        "5");
    }

  @Override
  public String getDescription() {
    return null;
  }
}