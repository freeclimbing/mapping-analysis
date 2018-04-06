package org.mappinganalysis.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.functions.QualityEdgeCreator;

import java.util.Properties;

public class QualityUtils {
  private static final Logger LOG = Logger.getLogger(QualityUtils.class);

//  public static void printQuality(
//      String dataset,
//      double mergeThreshold,
//      double simSortThreshold,
//      DataSet<Vertex<Long, ObjectMap>> merged,
//      String pmPath,
//      int sourcesCount) throws Exception {
//  }

  public static void printGeoQuality(
      DataSet<Vertex<Long, ObjectMap>> merged,
      Properties properties)
      throws Exception {
    Double mergeThreshold = properties.get(Constants.MERGE_THRESHOLD) == null
        ? 0d : (double) properties.get(Constants.MERGE_THRESHOLD);
    double simSortThreshold = properties.get(Constants.SIMSORT_THRESHOLD) == null
        ? 0d : (double) properties.get(Constants.SIMSORT_THRESHOLD);
    String dataset = properties.getProperty(Constants.DATASET);
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
          .runOperation(new EdgeComputationVertexCcSet(
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

    LOG.info("precision: " + precision + " recall: " + recall
        + " F1: " + 2 * precision * recall / (precision + recall));
    LOG.info("######################################################");
  }

}
