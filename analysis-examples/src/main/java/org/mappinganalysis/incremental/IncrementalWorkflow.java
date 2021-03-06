package org.mappinganalysis.incremental;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobExecutionResult;
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
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;

import java.util.concurrent.TimeUnit;

public class IncrementalWorkflow implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String PREPROCESSING_STEP = "incremental-preprocessing";
  private static final String MERGE_STEP = "incremental-merged-clusters";
  private static final String PRE_JOB = "Incremental Preprocessing";
  private static final String MER_JOB = "Incremental Merge";

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 3,
        "args[0]: input dir, " +
            "args[1]: domain (geo, music, nc), " +
            "args[2]: selection strategy (entity, source)");
    String INPUT_PATH = args[0];
    String domain = args[1];
    DataDomain dataDomain;
    switch (domain) {
      case Constants.GEO:
        dataDomain = DataDomain.GEOGRAPHY; // optional
        break;
      case Constants.MUSIC:
        dataDomain = DataDomain.MUSIC;
        break;
      case Constants.NC:
        dataDomain = DataDomain.NC;
        break;
      default:
        throw new IllegalArgumentException("Unsupported domain: " + domain);
    }

    String strategy = args[2];
    ClusteringStep clusteringStep;
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
//    String VERTEX_FILE_NAME = args[1];

    JobExecutionResult result;
    Constants.LL_MODE = "all";

    IncrementalConfig config = new IncrementalConfig(dataDomain, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(clusteringStep);
    config.setSimSortSimilarity(0.7);

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder(config)
//        .setMatchElements(newVertices)
//        .setNewSource(source)
        .build();

    Graph<Long, ObjectMap, NullValue> graph = new JSONDataSource(
        INPUT_PATH,
        Constants.LL_MODE.concat(Constants.INPUT_GRAPH),
        env)
        .getGraph(ObjectMap.class, NullValue.class);

    new JSONDataSink(INPUT_PATH, PREPROCESSING_STEP)
        .writeGraph(graph);
    result = env.execute(PRE_JOB);
    System.out.println(PRE_JOB + " needed "
        + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(INPUT_PATH, PREPROCESSING_STEP, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .run(clustering);

    new JSONDataSink(INPUT_PATH, MERGE_STEP)
        .writeVertices(vertices);
    result = env.execute(MER_JOB);
    System.out.println(MER_JOB + " needed "
        + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
  }

  @Override
  public String getDescription() {
    return null;
  }
}
