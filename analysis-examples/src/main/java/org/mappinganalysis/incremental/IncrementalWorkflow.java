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
import org.mappinganalysis.model.functions.IncrementalClustering;
import org.mappinganalysis.model.functions.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.preprocessing.IncrementalPreprocessing;
import org.mappinganalysis.util.Constants;

import java.util.concurrent.TimeUnit;

public class IncrementalWorkflow implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String PREPROCESSING_STEP = "incremental-preprocessing";
  private static final String MERGE_STEP = "incremental-merged-clusters";
  private static final String PRE_JOB = "Incremental Preprocessing";
  private static final String MER_JOB = "Incremental Merge";

  private static String INPUT_PATH;
  private static String VERTEX_FILE_NAME;

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2,
        "args[0]: input dir, " +
            "args[1]: file name");
    INPUT_PATH = args[0];
    VERTEX_FILE_NAME = args[1];

    Constants.SOURCE_COUNT = 5;
    DataDomain domain = DataDomain.GEOGRAPHY;
    JobExecutionResult result;

    /*
      incremental preprocessing
     */
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(
        Constants.INPUT_PATH,
        Constants.LL_MODE.concat(Constants.INPUT_GRAPH), env)
        .getGraph(ObjectMap.class, NullValue.class)
        // todo adapt preprocessing
        .run(new IncrementalPreprocessing(env));

    new JSONDataSink(INPUT_PATH, PREPROCESSING_STEP)
        .writeGraph(graph);
    result = env.execute(PRE_JOB);
    System.out.println(PRE_JOB + " needed "
        + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");

    /*
      incremental clustering
     */

    /*
      1. Select sources to match first
      2. Find representative, merge attributes

      take more care of decisions/provenance information
     */

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED)
        .build();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(INPUT_PATH, PREPROCESSING_STEP, env)
            .getGraph()
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
