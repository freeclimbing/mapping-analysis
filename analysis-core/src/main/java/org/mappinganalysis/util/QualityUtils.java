package org.mappinganalysis.util;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.functions.QualityEdgeCreator;

import java.math.BigDecimal;
import java.util.HashMap;

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
//    LOG.info("gold links: " + goldCount); // new execution
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount; // tp / (tp + fp)
    double recall = (double) tpCount / goldCount; // tp / (fn + tp)
    LOG.info("\n############### dataset: " + dataset
        + " mergeThreshold: " + mergeThreshold
        + " simSortThreshold: " + simSortThreshold);
    LOG.info("TP+FN: " + goldCount);
    LOG.info("TP+FP: " + checkCount);
    LOG.info("TP: " + tpCount);

    double f1 = 2 * precision * recall / (precision + recall);
    LOG.info("precision: " + precision + " recall: " + recall + " F1: " + f1);
    LOG.info("######################################################");

    HashMap<String, BigDecimal> result = Maps.newHashMap();
    result.put("precision", new BigDecimal(precision));
    result.put("recall", new BigDecimal(recall));
    result.put("f1", new BigDecimal(f1));

    return result;
  }

  public static HashMap<String, BigDecimal> printMusicQuality(
      DataSet<Vertex<Long, ObjectMap>> checkClusters,
      Config config)
      throws Exception {
    DataSet<Tuple2<Long, Long>> clusterEdges = checkClusters
        .flatMap(new QualityEdgeCreator());

    String path = "/data/musicbrainz/input/";
    DataSet<Tuple2<Long, Long>> goldLinks;

    String pmPath = QualityUtils.class.getResource(path).getFile();

      DataSet<Tuple2<String, String>> perfectMapping = config
          .getExecutionEnvironment()
          .readCsvFile(pmPath.concat("musicbrainz-20000-A01.csv.dapo"))
          .ignoreFirstLine()
          .includeFields(true, true, false, false, false, false, false, false, false, false, false, false)
          .types(String.class, String.class);

      goldLinks = perfectMapping
          .map(tuple -> new Vertex<>(Long.parseLong(tuple.f0), Long.parseLong(tuple.f1)))
          .returns(new TypeHint<Vertex<Long, Long>>() {})
          .groupBy(1)
          .reduceGroup(new AllEdgesCreateGroupReducer<>())
          .map(edge -> new Tuple2<>(edge.getSource(), edge.getTarget()))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks
        .join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
            return first;
          }
        });

    long goldCount = goldLinks.count();
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount; // tp / (tp + fp)
    double recall = (double) tpCount / goldCount; // tp / (fn + tp)
    LOG.info("\n############### dataset: " + config.toString());
    LOG.info("TP+FN: " + goldCount);
    LOG.info("TP+FP: " + checkCount);
    LOG.info("TP: " + tpCount);

    double f1 = 2 * precision * recall / (precision + recall);
    LOG.info("precision: " + precision + " recall: " + recall + " F1: " + f1);
    LOG.info("######################################################");

    HashMap<String, BigDecimal> result = Maps.newHashMap();
    result.put("precision", new BigDecimal(precision));
    result.put("recall", new BigDecimal(recall));
    result.put("f1", new BigDecimal(f1));

    return result;
  }

}
