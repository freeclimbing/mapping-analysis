package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.FullOuterJoinSimilarityValueFunction;
import org.mappinganalysis.model.functions.simsort.AggSimValueEdgeMapFunction;
import org.mappinganalysis.model.functions.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.utils.Utils;

public class SimCompUtility {
  private static final Logger LOG = Logger.getLogger(SimCompUtility.class);

  public static DataSet<Triplet<Long, ObjectMap, ObjectMap>> computeSimilarities(DataSet<Triplet<Long, ObjectMap,
      NullValue>> triplets, String filter) {
    LOG.info("Started: compute similarities...");
    switch (filter) {
      case "geo":
        return basicGeoSimilarity(triplets);
      case "label":
        return basicTrigramSimilarity(triplets);
      case "type":
        return basicTypeSimilarity(triplets);
      default:
        return joinDifferentSimilarityValues(basicGeoSimilarity(triplets),
            basicTrigramSimilarity(triplets),
            basicTypeSimilarity(triplets));
    }
  }

  /**
   * Compute similarities based on the existing vertex properties, save aggregated similarity as edge property
   * @param graph input graph
   * @return graph with edge similarities
   */
  public static DataSet<Edge<Long, ObjectMap>> computeEdgeSimWithVertices(Graph<Long, ObjectMap, NullValue> graph) {
    LOG.info("Start computing edge similarities...");
    final DataSet<Triplet<Long, ObjectMap, ObjectMap>> accumulatedSimValueTriplets
        = computeSimilarities(graph.getTriplets(), Utils.PRE_CLUSTER_STRATEGY);
    LOG.info("Done.");

    return accumulatedSimValueTriplets
        .map(new TripletToEdgeMapFunction())
        .map(new AggSimValueEdgeMapFunction(Utils.IGNORE_MISSING_PROPERTIES));
  }

  /**
   * Join several sets of triplets which are being produced within property similarity computation.
   * Edges where no similarity value is higher than the appropriate threshold are not in the result set.
   * @param tripletDataSet input data sets
   * @return joined dataset with all similarities in an ObjectMap
   */
  @SafeVarargs
  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> joinDifferentSimilarityValues(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>>... tripletDataSet) {
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> triplets = null;
    boolean isFirstSet = false;
    for (DataSet<Triplet<Long, ObjectMap, ObjectMap>> dataSet : tripletDataSet) {
      if (!isFirstSet) {
        triplets = dataSet;
        isFirstSet = true;
      } else {
        triplets = triplets
            .fullOuterJoin(dataSet)
            .where(0, 1)
            .equalTo(0, 1)
            .with(new FullOuterJoinSimilarityValueFunction());
      }
    }
    return triplets;
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTypeSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets
        .map(new TypeSimilarityMapper());
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTrigramSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets
        .map(new TrigramSimilarityMapper());
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicGeoSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets
        .filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimMapper(100000));
  }
}
