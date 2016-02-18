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

  /**
   * Decide which similarities should be computed based on filter
   * @param triplets graph triplets
   * @param filter strategy: geo, label, type, [empty, combined] -> all 3 combined
   * @return triplets with sim values
   */
  public static DataSet<Triplet<Long, ObjectMap, ObjectMap>> computeSimilarities(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets, String filter) {
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
    LOG.info("Compute Edge similarities based on vertex values, ignore missing properties: " + Utils.IGNORE_MISSING_PROPERTIES);
    return computeSimilarities(graph.getTriplets(), Utils.PRE_CLUSTER_STRATEGY)
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
    return triplets.map(new TypeSimilarityMapper());
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTrigramSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets.map(new TrigramSimilarityMapper());
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicGeoSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets.filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimMapper(Utils.MAXIMAL_GEO_DISTANCE));
  }

  /**
   * Get a new triplet with an empty ObjectMap as edge value.
   * @param triplet triplet where edge value is NullValue
   * @return result triplet
   */
  public static Triplet<Long, ObjectMap, ObjectMap> initResultTriplet(Triplet<Long, ObjectMap, NullValue> triplet) {
    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            new ObjectMap()));
  }
}
