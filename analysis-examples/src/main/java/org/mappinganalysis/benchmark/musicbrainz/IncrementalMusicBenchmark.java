package org.mappinganalysis.benchmark.musicbrainz;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.incremental.MatchStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.ExecutionUtils;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class IncrementalMusicBenchmark
    implements ProgramDescription {
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
    String inputPath = args[0];
    final String vertexFileName = args[1];
    final String strategy = args[2];
    final String FULL_OR_EVAL = args[5];
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
    final double threshold = Double.valueOf(args[3]);
    final int blockingLength = Integer.valueOf(args[4]);

    MatchStrategy matchStrategy = MatchStrategy.MAX_BOTH;
    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(clusteringStep);
//    config.setSimSortSimilarity(0.7); // not currently needed
    config.setMinResultSimilarity(threshold);
    config.setMatchStrategy(matchStrategy);
    config.setBlockingLength(blockingLength);

    String jobName = ExecutionUtils.setJobName(config);

    if (vertexFileName.contains("20000000")) {
      jobName = "-20mio-".concat(jobName);
    } else if (vertexFileName.contains("2000000")) {
      jobName = "-2mio-".concat(jobName);
    } else {
      jobName = "-20k-".concat(jobName);
    }

    if (FULL_OR_EVAL.equals("full")) {

      // read base graph
      Graph<Long, ObjectMap, NullValue> baseGraph
          = new CSVDataSource(inputPath, vertexFileName, env)
          .getGraph();

      DataSet<Vertex<Long, ObjectMap>> newVertices = baseGraph.getVertices()
          .filter(new SourceFilterFunction("2"));

      IncrementalClustering initialClustering = new IncrementalClustering
          .IncrementalClusteringBuilder(config)
          .setMatchElements(newVertices)
          .setNewSource("2")
          .build();

      Graph<Long, ObjectMap, NullValue> startingGraph = baseGraph
          .filterOnVertices(new SourceFilterFunction("1"));

      DataSet<Vertex<Long, ObjectMap>> clusters = startingGraph
          .run(initialClustering);

      new JSONDataSink(inputPath, "1+2".concat(jobName))
          .writeVertices(clusters);
      JobExecutionResult result = env.execute("1+2".concat(jobName));

      System.out.println("1+2".concat(jobName) + " needed "
          + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
      QualityUtils.printExecPlusAccumulatorResults(result);

    /*
      +3
     */
      startingGraph = new JSONDataSource(inputPath, "1+2".concat(jobName), env)
          .getGraph(ObjectMap.class, NullValue.class);

      newVertices = baseGraph.getVertices()
          .filter(new SourceFilterFunction("3"));

      IncrementalClustering addThreeClustering = new IncrementalClustering
          .IncrementalClusteringBuilder(config)
          .setMatchElements(newVertices)
          .setNewSource("3")
          .build();

      clusters = startingGraph
          .run(addThreeClustering);

      new JSONDataSink(inputPath, "+3".concat(jobName))
          .writeVertices(clusters);
      result = env.execute("+3".concat(jobName));

      System.out.println("+3".concat(jobName) + " needed "
          + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
      QualityUtils.printExecPlusAccumulatorResults(result);

    /*
      +4
     */
      startingGraph = new JSONDataSource(inputPath, "+3".concat(jobName), env)
          .getGraph(ObjectMap.class, NullValue.class);

      newVertices = baseGraph.getVertices()
          .filter(new SourceFilterFunction("4"));

      IncrementalClustering addFourClustering = new IncrementalClustering
          .IncrementalClusteringBuilder(config)
          .setMatchElements(newVertices)
          .setNewSource("4")
          .build();

      clusters = startingGraph
          .run(addFourClustering);

      new JSONDataSink(inputPath, "+4".concat(jobName))
          .writeVertices(clusters);
      result = env.execute("+4".concat(jobName));

      System.out.println("+4".concat(jobName) + " needed "
          + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
      QualityUtils.printExecPlusAccumulatorResults(result);

    /*
      +5
     */
      startingGraph = new JSONDataSource(inputPath, "+4".concat(jobName), env)
          .getGraph(ObjectMap.class, NullValue.class);

      newVertices = baseGraph.getVertices()
          .filter(new SourceFilterFunction("5"));

      IncrementalClustering addFiveClustering = new IncrementalClustering
          .IncrementalClusteringBuilder(config)
          .setMatchElements(newVertices)
          .setNewSource("5")
          .build();

      clusters = startingGraph
          .run(addFiveClustering);

      new JSONDataSink(inputPath, "+5".concat(jobName))
          .writeVertices(clusters);
      result = env.execute("+5".concat(jobName));

      System.out.println("+5".concat(jobName) + " needed "
          + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
      QualityUtils.printExecPlusAccumulatorResults(result);
    }

    /*
      Adaptation to evaluate single results.
     */
    if (FULL_OR_EVAL.equals("eval")) {
      if (inputPath.endsWith("/")) {
        inputPath = inputPath.substring(0, inputPath.length() - 1);
      }

      Iterator<String> split = Splitter.on('/').split(inputPath).iterator();
      jobName = "";

      while (split.hasNext()) {
        jobName = split.next();
      }

      inputPath = inputPath.substring(0, inputPath.length() - jobName.length());
    }

    System.out.println("statistics input path: " + inputPath + " job name: " + jobName);

    // quality
    Graph<Long, ObjectMap, NullValue> statisticsGraph
        = new JSONDataSource(inputPath, jobName, env)
        .getGraph(ObjectMap.class, NullValue.class);

    QualityUtils.printMusicQuality(
        statisticsGraph.getVertices(),
        config,
        inputPath,
        vertexFileName,
        "cluster");
  }

  @Override
  public String getDescription() {
    return null;
  }
}