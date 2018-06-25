package org.mappinganalysis.benchmark.musicbrainz;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.io.impl.json.JSONToEdgeFormatter;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class IncrementalMusicBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String PREPROCESSING_STEP = "incremental-preprocessing";
  private static final String MERGE_STEP = "incremental-merged-clusters";
  private static final String PRE_JOB = "Incremental Preprocessing";
  private static final String MER_JOB = "Incremental Merge";

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 3,
        "args[0]: input dir, " +
            "args[1]: file name, " +
            "args[2]: selection strategy (entity, source)");
    final String inputPath = args[0];
    final String vertexFileName = args[1];
    final DataDomain dataDomain = DataDomain.MUSIC;

    String strategy = args[2];
    final ClusteringStep clusteringStep;
    switch (strategy) {
      case "entity":
        clusteringStep = ClusteringStep.VERTEX_ADDITION;
        break;
      case "source":
        clusteringStep = ClusteringStep.SOURCE_ADDITION;
        break;
      default:
        throw new IllegalArgumentException("Unsupported step: " + strategy);
    }

    JobExecutionResult result;

    IncrementalConfig config = new IncrementalConfig(dataDomain, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(clusteringStep);
    config.setSimSortSimilarity(0.7);

    // read base graph
    Graph<Long, ObjectMap, NullValue> baseGraph =
        new CSVDataSource(inputPath, vertexFileName, env)
            .getGraph();

    List<String> musicSources = Constants.MUSIC_SOURCES;
    Graph<Long, ObjectMap, NullValue> workingGraph = null;
    DataSet<Vertex<Long, ObjectMap>> clusters;

    boolean isFirst = true;
    for (String musicSource : musicSources) {
      if (isFirst) {
        workingGraph = baseGraph.filterOnVertices(new SourceFilterFunction(musicSource));
        isFirst = false;
      } else {
        DataSet<Vertex<Long, ObjectMap>> newVertices = baseGraph
            .getVertices()
            .filter(new SourceFilterFunction(musicSource));

        IncrementalClustering clustering = new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .setNewSource(musicSource)
            .build();

        clusters = workingGraph.run(clustering);

        DataSet<Edge<Long, NullValue>> edges = env.fromCollection(
            Lists.newArrayList(""))
            .map(new JSONToEdgeFormatter<>(NullValue.class));

        workingGraph = Graph.fromDataSet(clusters, edges, env);
      }
    }

    result = env.execute(MER_JOB);
    System.out.println(MER_JOB + " needed "
        + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
  }

  @Override
  public String getDescription() {
    return null;
  }
}