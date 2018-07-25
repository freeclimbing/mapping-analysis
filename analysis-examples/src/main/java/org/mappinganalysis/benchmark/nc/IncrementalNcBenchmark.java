package org.mappinganalysis.benchmark.nc;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;
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
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.incremental.MatchingStrategy;
import org.mappinganalysis.model.functions.stats.StatisticsClusterCounterRichMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.ExecutionUtils;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IncrementalNcBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment
      .getExecutionEnvironment();

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 6,
        "args[0]: input dir, "
            + "args[1]: file name, "
            + "args[2]: selection strategy (entity, source)"
            + "args[3]: threshold, "
            + "args[4]: blockingLength, "
            + "args[5]: run [eval/full]");
    String INPUT_PATH = args[0].concat(args[1]);
    final String STRATEGY = args[2];
    final String FULL_OR_EVAL = args[5];
    final ClusteringStep CLUSTERING_STEP;
    switch (STRATEGY) {
      case "entity":
        CLUSTERING_STEP = ClusteringStep.VERTEX_ADDITION;
        break;
      case "source":
        CLUSTERING_STEP = ClusteringStep.SOURCE_ADDITION;
        break;
      default:
        throw new IllegalArgumentException("Unsupported step: " + STRATEGY);
    }

    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(CLUSTERING_STEP);
    config.setMinResultSimilarity(Double.valueOf(args[3]));
    config.setMatchStrategy(MatchingStrategy.MAX_BOTH);
    config.setBlockingLength(Integer.valueOf(args[4]));

    String jobName = ExecutionUtils.setJobName(config);
    System.out.println("Now running ... " + jobName);

    boolean isFirst = true;
    boolean isSecond = true;
    Graph<Long, ObjectMap, NullValue> inputGraph = null;
    DataSet<Vertex<Long, ObjectMap>> newVertices;
    DataSet<Vertex<Long, ObjectMap>> clusters;

    final LogicalGraph logicalGraph = Utils
        .getGradoopGraph(INPUT_PATH, env);
    final Graph<Long, ObjectMap, NullValue> baseGraph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    List<String> sources;
    if (INPUT_PATH.contains("5")) {
      sources = Lists.newArrayList(Constants.NC_1, Constants.NC_2,
          Constants.NC_3, Constants.NC_4, Constants.NC_5);
    } else {
      sources = Constants.NC_SOURCES;
    }

    if (FULL_OR_EVAL.equals("full")) {
      long completeTime = 0;
      for (String source : sources) {
        if (isFirst) {
          inputGraph = baseGraph
              .filterOnVertices(new SourceFilterFunction(source));
          jobName = source.replaceAll("[^\\d.]", "")
              .concat(jobName);
          isFirst = false;
        } else {
          if (isSecond) {
            jobName = source.replaceAll("[^\\d.]", "")
                .concat(jobName);
            isSecond = false;
          } else {
            inputGraph = new JSONDataSource(INPUT_PATH, jobName, env)
                .getGraph(ObjectMap.class, NullValue.class);
            jobName = source.replaceAll("[^\\d.]", "")
                .concat(jobName);
          }

          newVertices = baseGraph.getVertices()
              .filter(new SourceFilterFunction(source));

          IncrementalClustering clustering = new IncrementalClustering
              .IncrementalClusteringBuilder(config)
              .setNewSource(source)
              .setMatchElements(newVertices)
              .build();

          clusters = inputGraph.run(clustering);

          new JSONDataSink(INPUT_PATH, jobName)
              .writeVertices(clusters);
          JobExecutionResult execResult = env.execute(jobName);

          long netRuntimeSecs = execResult.getNetRuntime(TimeUnit.SECONDS);
          completeTime += netRuntimeSecs;
          System.out.println(jobName + " needs "
              + netRuntimeSecs + " seconds.");

          QualityUtils.printExecPlusAccumulatorResults(execResult);
        }
      }

      System.out.println("Overall time for all parts of "
          + jobName + ": " + completeTime + "s.");
    }

    /*
      Adaptation to evaluate single results.
     */
    if (FULL_OR_EVAL.equals("eval")) {
      if (INPUT_PATH.endsWith("/")) {
        INPUT_PATH = INPUT_PATH.substring(0, INPUT_PATH.length() - 1);
      }

      Iterator<String> split = Splitter.on('/').split(INPUT_PATH).iterator();
      jobName = "";

      while (split.hasNext()) {
        jobName = split.next();
      }

      INPUT_PATH = INPUT_PATH.substring(0, INPUT_PATH.length() - jobName.length());
    }

      // quality
      Graph<Long, ObjectMap, NullValue> statisticsGraph
          = new JSONDataSource(INPUT_PATH, jobName, env)
          .getGraph(ObjectMap.class, NullValue.class);

      QualityUtils.printNcQuality(
          statisticsGraph.getVertices()
              .map(new StatisticsClusterCounterRichMapFunction("eval-")),
          config,
          INPUT_PATH,
          "cluster",
          jobName);

  }

  @Override
  public String getDescription() {
    return null;
  }
}