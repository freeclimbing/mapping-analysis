package org.mappinganalysis.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.stats.StatisticsClusterCounterRichMapFunction;
import org.mappinganalysis.model.functions.stats.StatisticsCountElementsRichMapFunction;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.functions.QualityEdgeCreator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class QualityUtils {
  private static final Logger LOG = Logger.getLogger(QualityUtils.class);

  public static HashMap<String, BigDecimal> printGeoQuality(
      DataSet<Vertex<Long, ObjectMap>> merged,
      Config properties)
      throws Exception {
    Double mergeThreshold = properties.get(Constants.MERGE_THRESHOLD) == null
        ? 0d : (double) properties.get(Constants.MERGE_THRESHOLD);
    double simSortThreshold = properties.get(Constants.SIMSORT_THRESHOLD) == null
        ? 0d : (double) properties.get(Constants.SIMSORT_THRESHOLD);
    String dataset = properties.getProperty(Constants.DATASET, Constants.EMPTY_STRING);
    ExecutionEnvironment env = (ExecutionEnvironment) properties.get(Constants.ENV);

    /*
      set merge threshold to 0 to have representative in output label (cosmetic)
     */
    if (mergeThreshold == 0.0) {
      dataset = dataset.concat("REPR");
    }
    DataSet<Tuple2<Long, Long>> clusterEdges = merged
        .flatMap(new QualityEdgeCreator());

    String pmPath = QualityUtils.class
          .getResource("/data/settlement-benchmark/gold/").getFile();

    DataSet<Tuple2<Long, Long>> goldLinks = new JSONDataSource(
          pmPath, true, env)
          .getGraph(ObjectMap.class, NullValue.class)
          .getVertices()
          .runOperation(new EdgeComputationOnVerticesForKeySelector(
              null,
              EdgeComputationStrategy.REPRESENTATIVE))
          .map(edge -> new Tuple2<>(edge.getSource(), edge.getTarget()))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks
        .join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with((first, second) -> first)
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    long goldCount = goldLinks.count();
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    // todo get config + jobname
    return getAndprintRecallPrecisionFM(
        null,
        "jobname",
        goldCount,
        checkCount,
        tpCount);
  }

  /**
   * Print music dataset quality for different sizes, use Accumulators.
   * @param checkClusters vertices to be checked
   * @param config execution config
   * @param inputPath csv input path to read from
   * @param vertexFileName dataset name
   * @return resulting quality f1
   */
  public static HashMap<String, BigDecimal> printMusicQuality(
      DataSet<Vertex<Long, ObjectMap>> checkClusters,
      Config config,
      String inputPath,
      String vertexFileName,
      String mode) throws Exception {
    DataSet<Tuple2<Long, Long>> clusterEdges = checkClusters
        .map(new StatisticsClusterCounterRichMapFunction("gold-"))
        .flatMap(new QualityEdgeCreator())
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.TEST_LINKS_ACCUMULATOR));

    DataSet<Tuple2<String, String>> perfectMapping;
    String readFromPath;
    if (vertexFileName.contains("2000000")) {
      if (!inputPath.endsWith(Constants.SLASH)) {
        readFromPath = inputPath.concat(Constants.SLASH);
      } else {
        readFromPath = inputPath;
      }
      readFromPath = readFromPath.concat("input/").concat(vertexFileName);
    } else {
      readFromPath = QualityUtils.class
          .getResource("/data/musicbrainz/input/")
          .getFile()
          .concat("musicbrainz-20000-A01.csv.dapo");
    }

    perfectMapping = config
        .getExecutionEnvironment()
        .readCsvFile(readFromPath)
        .ignoreFirstLine()
        .includeFields(true, true, false, false, false, false, false, false, false,
            false, false, false)
        .types(String.class, String.class);

    DataSet<Tuple2<Long, Long>> goldLinks = perfectMapping
        .map(tuple -> new Vertex<>(Long.parseLong(tuple.f0), Long.parseLong(tuple.f1)))
        .returns(new TypeHint<Vertex<Long, Long>>() {})
        .groupBy(1)
        .reduceGroup(new AllEdgesCreateGroupReducer<>())
        .map(edge -> new Tuple2<>(edge.getSource(), edge.getTarget()))
        .returns(new TypeHint<Tuple2<Long, Long>>() {})
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.GOLD_LINKS_ACCUMULATOR));

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks
        .join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with(new GetLeftSideJoinFunction())
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.TRUE_POSITIVE_ACCUMULATOR));

    if (mode.equals("cluster")) {
      new JSONDataSink(inputPath, "statistics")
          .writeTuples(truePositives);
    } else {
      ArrayList<Tuple2<Long, Long>> representatives = Lists.newArrayList();
      truePositives.output(new LocalCollectionOutputFormat<>(representatives));
    }

    JobExecutionResult jobResult = config
        .getExecutionEnvironment()
        .execute("statistics");

    QualityUtils.printExecPlusAccumulatorResults(jobResult);

    return getAndprintRecallPrecisionFM(
        config,
        vertexFileName,
        jobResult.getAccumulatorResult(Constants.GOLD_LINKS_ACCUMULATOR),
        jobResult.getAccumulatorResult(Constants.TEST_LINKS_ACCUMULATOR),
        jobResult.getAccumulatorResult(Constants.TRUE_POSITIVE_ACCUMULATOR));
  }

  private static HashMap<String, BigDecimal> getAndprintRecallPrecisionFM(
      Config config,
      String jobName,
      long goldCount,
      long checkCount,
      long tpCount) {
    double precision = Utils.getExactDoubleResult(tpCount, checkCount, 4);
    double recall = Utils.getExactDoubleResult(tpCount, goldCount, 4);
    System.out.println("\n############### job: " + jobName + " config: " + config.toString());
    System.out.println("TP+FN: " + goldCount);
    System.out.println("TP+FP: " + checkCount);
    System.out.println("TP: " + tpCount);

    double f1 = Utils.getExactDoubleResult(
        2 * precision * recall,
        precision + recall,
        4);
    System.out.println("precision: " + precision + " recall: " + recall + " F1: " + f1);
    System.out.println("######################################################");

    HashMap<String, BigDecimal> result = Maps.newHashMap();
    result.put("precision", new BigDecimal(precision));
    result.put("recall", new BigDecimal(recall));
    result.put("f1", new BigDecimal(f1));

    return result;
  }

  public static void printNcQuality(
      DataSet<Vertex<Long, ObjectMap>> checkClusters,
      Config config,
      String inputPath,
      String clusterExecution,
      String jobName) throws Exception {

    System.out.println("Printing evaluation for job: "
        + jobName + " on input path: " + inputPath + " config: " + config.toString());

    DataSet<Tuple2<Long, Long>> clusterEdges = checkClusters
        .map(new StatisticsClusterCounterRichMapFunction("gold-"))
        .flatMap(new QualityEdgeCreator())
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.TEST_LINKS_ACCUMULATOR));

    DataSet<Tuple2<Long, Long>> goldLinks;

    if (clusterExecution.equals("local")) {
      int sourcesCount;
      if (jobName.contains("10")) {
        sourcesCount = 10;
      } else {
        sourcesCount = 5;
      }

      String path = QualityUtils.class
          .getResource("/data/nc/" + sourcesCount + "pm/")
          .getFile();

      DataSet<Tuple2<String, String>> perfectMapping = config.getExecutionEnvironment()
          .readCsvFile(path.concat("pm.csv"))
          .types(String.class, String.class);

      goldLinks = perfectMapping
          .map(new NcPmMapFunction());
    } else { // get pm edges from hdfs
      goldLinks = getPmEdges(inputPath, config.getExecutionEnvironment())
          .map(edge -> new Tuple2<>(edge.f0, edge.f1))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});
    }

    goldLinks = goldLinks
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.GOLD_LINKS_ACCUMULATOR));

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks
        .join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with(new GetLeftSideJoinFunction())
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.TRUE_POSITIVE_ACCUMULATOR));

    new JSONDataSink(inputPath, jobName.concat("tp"))
        .writeTuples(truePositives);

    JobExecutionResult jobResult = config
        .getExecutionEnvironment()
        .execute("statistics-".concat(jobName));



    QualityUtils.printExecPlusAccumulatorResults(jobResult);

    getAndprintRecallPrecisionFM(
        config,
        jobName,
        jobResult.getAccumulatorResult(Constants.GOLD_LINKS_ACCUMULATOR),
        jobResult.getAccumulatorResult(Constants.TEST_LINKS_ACCUMULATOR),
        jobResult.getAccumulatorResult(Constants.TRUE_POSITIVE_ACCUMULATOR));
  }


  /**
   * old default quality determination
   */
  @Deprecated
  public static void printQuality(
      String dataset,
      double mergeThreshold,
      double simSortThreshold,
      DataSet<Vertex<Long, ObjectMap>> merged,
      String pmPath,
      int sourcesCount,
      ExecutionEnvironment env) throws Exception {
    DataSet<Tuple2<Long, Long>> clusterEdges = merged
        .flatMap(new QualityEdgeCreator());

    String path = "/data/nc/" + sourcesCount + "pm/";
    DataSet<Tuple2<Long, Long>> goldLinks;

    if (pmPath.equals(Constants.EMPTY_STRING)) {
      pmPath = QualityUtils.class
          .getResource(path).getFile();

      DataSet<Tuple2<String, String>> perfectMapping = env
          .readCsvFile(pmPath.concat("pm.csv"))
          .types(String.class, String.class);

      goldLinks = perfectMapping
          .map(new NcPmMapFunction());
    } else {
      goldLinks = getPmEdges(pmPath, env)
          .map(edge -> new Tuple2<>(edge.f0, edge.f1))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});
    }

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks
        .join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with(new GetLeftSideJoinFunction());

    long goldCount = goldLinks.count();
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount;
    double recall = (double) tpCount / goldCount;
    LOG.info("\n############### dataset: " + dataset + " mergeThreshold: " + mergeThreshold + " simSortThreshold: " + simSortThreshold);
    LOG.info("TP+FN: " + goldCount);
    LOG.info("TP+FP: " + checkCount);
    LOG.info("TP: " + tpCount);

    LOG.info("precision: " + precision + " recall: " + recall
        + " F1: " + 2 * precision * recall / (precision + recall));
    LOG.info("######################################################");
  }

  /**
   * For a (hdfs) path, retrieve Gradoop graph for NC dataset  and transform to
   * a set of (gold) edges via component ids for evaluation purposes.
   */
  private static DataSet<Edge<Long, NullValue>> getPmEdges(
      String graphPath,
      ExecutionEnvironment env) throws Exception {
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);

    DataSet<Tuple2<Long, Long>> clsIds = Utils.getInputGraph(logicalGraph, Constants.NC, env)
        .getVertices()
        .map(vertex -> new Tuple2<>(vertex.getId(),
            (long) vertex.getValue().get("clsId")))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    return clsIds
        .groupBy(1) // clsId
        .reduceGroup(new AllEdgesCreateGroupReducer<>("gold-"));
  }

  public static void printExecPlusAccumulatorResults(JobExecutionResult execResult) {
    Map<String, Object> allAccumulatorResults = execResult.getAllAccumulatorResults();

    for (Map.Entry<String, Object> stringObjectEntry : allAccumulatorResults.entrySet()) {
      if (stringObjectEntry.getValue() instanceof Long) {
        long value = Long.parseLong(stringObjectEntry.getValue().toString());
        if (value != 0L) {
          System.out.println(stringObjectEntry.getKey() + " = " + value);
        }
      } else {
        System.out.println(stringObjectEntry);
      }
    }
  }

  private static class NcPmMapFunction implements MapFunction<Tuple2<String, String>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> map(Tuple2<String, String> pmValue) throws Exception {
      long first = Utils.getIdFromNcId(pmValue.f0);
      long second = Utils.getIdFromNcId(pmValue.f1);

      if (first < second) {
        return new Tuple2<>(first, second);
      } else {
        return new Tuple2<>(second, first);
      }
    }
  }

  private static class GetLeftSideJoinFunction
      implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
      return first;
    }
  }
}
