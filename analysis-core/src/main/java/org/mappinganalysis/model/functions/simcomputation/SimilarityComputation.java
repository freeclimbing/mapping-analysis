package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.debug.PrintVertices;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.FullOuterJoinSimilarityValueFunction;
import org.mappinganalysis.model.functions.simsort.SimSort;
import org.mappinganalysis.model.functions.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.functions.typegroupby.TypeGroupBy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.math.BigDecimal;

public class SimilarityComputation {
  private static final Logger LOG = Logger.getLogger(SimilarityComputation.class);

  /**
   * Decide which similarities should be computed based on filter
   * @param triplets graph triplets
   * @param filter strategy: geo, label, type, [empty, combined] -> all 3 combined
   * @return triplets with sim values
   */
  public static DataSet<Triplet<Long, ObjectMap, ObjectMap>> computeSimilarities(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets, String filter) {
    switch (filter) {
      case Constants.SIM_GEO_LABEL_STRATEGY:
        return joinDifferentSimilarityValues(basicGeoSimilarity(triplets),
            basicTrigramSimilarity(triplets));
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
   * @param matchCombination relevant: Utils.SIM_GEO_LABEL_STRATEGY or Utils.DEFAULT_VALUE
   * @return graph with edge similarities
   */
  public static DataSet<Edge<Long, ObjectMap>> computeGraphEdgeSim(Graph<Long, ObjectMap, NullValue> graph,
                                                                   String matchCombination) {
    LOG.info("Compute Edge similarities based on vertex values, ignore missing properties: "
        + Constants.IGNORE_MISSING_PROPERTIES);

    return computeSimilarities(graph.getTriplets(), matchCombination)
        .map(new TripletToEdgeMapFunction())
        .map(new AggSimValueEdgeMapFunction(Constants.IGNORE_MISSING_PROPERTIES));
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
        .map(new GeoCodeSimMapper(Constants.MAXIMAL_GEO_DISTANCE));
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

  /**
   * Compose similarity values based on existence: if property is missing, its not considered at all.
   * @param value property map
   * @return mean similarity value
   */
  public static double getMeanSimilarity(ObjectMap value) {
    double aggregatedSim = 0;
    int propCount = 0;
    if (value.containsKey(Constants.SIM_TRIGRAM)) {
      ++propCount;
      aggregatedSim = (double) value.get(Constants.SIM_TRIGRAM);
    }
    if (value.containsKey(Constants.SIM_TYPE)) {
      ++propCount;
      aggregatedSim += (double) value.get(Constants.SIM_TYPE);
    }
    if (value.containsKey(Constants.SIM_DISTANCE)) {
      double distanceSim = getDistanceValue(value);
      if (Doubles.compare(distanceSim, -1) > 0) {
        aggregatedSim += distanceSim;
        ++propCount;
      }
    }

    Preconditions.checkArgument(propCount != 0, "prop count 0 for objectmap: " + value.toString());

    BigDecimal result = new BigDecimal(aggregatedSim / propCount);
    result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

    return result.doubleValue();
  }

  /**
   * Compose similarity values based on weights for each of the properties, missing values are counted as zero.
   * @param values property map
   * @return aggregated similarity value
   */
  public static double getWeightedAggSim(ObjectMap values) {
    double trigramWeight = 0.45;
    double typeWeight = 0.25;
    double geoWeight = 0.3;
    double aggregatedSim;
    if (values.containsKey(Constants.SIM_TRIGRAM)) {
      aggregatedSim = trigramWeight * (double) values.get(Constants.SIM_TRIGRAM);
    } else {
      aggregatedSim = 0;
    }
    if (values.containsKey(Constants.SIM_TYPE)) {
      aggregatedSim += typeWeight * (double) values.get(Constants.SIM_TYPE);
    }
    if (values.containsKey(Constants.SIM_DISTANCE)) {
      double distanceSim = getDistanceValue(values);
      if (Doubles.compare(distanceSim, -1) > 0) {
        aggregatedSim += geoWeight * distanceSim;
      }
    }

    BigDecimal result = new BigDecimal(aggregatedSim);
    result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

    return result.doubleValue();
  }

  /**
   * get distance property from object map TODO check if needed
   * @param value object map
   * @return distance
   */
  private static double getDistanceValue(ObjectMap value) {
    Object object = value.get(Constants.SIM_DISTANCE);
    Preconditions.checkArgument(object instanceof Double, "Error (should not occur)" + object.getClass().toString());

    return (Double) object;
  }

  /**
   * Executes TypeGroupBy and SimSort method and returns the resulting graph.
   * @param graph graph with similarities already computed
   * @param processingMode if 'default', both methods are executed
   * @param env env  @return result
   */
  public static Graph<Long, ObjectMap, ObjectMap> executeAdvanced(Graph<Long, ObjectMap, ObjectMap> graph,
                                                                  String processingMode, ExecutionEnvironment env,
                                                                  ExampleOutput out) throws Exception {
    LOG.setLevel(Level.DEBUG);
    /*
     * sync begin
     */
    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices().filter(value -> true)
        .map(new PrintVertices(false, "preTGB"));
    DataSet<Edge<Long, ObjectMap>> edges = graph.getEdges().filter(value -> true);
    graph = Graph.fromDataSet(vertices, edges, env);
    /*
     * sync end
     */

    // internally compType is used, afterwards typeIntern is used again
    graph = TypeGroupBy.execute(graph, processingMode, 1000, env, out);

//    Utils.writeToHdfs(graph.getVertices(), "4_post_sim_sort");
//    env.execute();

    /*
     * sync begin
     */
    vertices = graph.getVertices().filter(value -> {
      LOG.info("filtered vertex " + value.toString());
      return true;
    });
    edges = graph.getEdges().filter(value -> {
      LOG.info("filtered edge");
      return true;
    });
    graph = Graph.fromDataSet(vertices, edges, env);
    /*
     * sync end
     */

    /*
     * SimSort (and postprocessing TypeGroupBy in prepare)
     */
    graph = SimSort.prepare(graph, processingMode, env, out);
    Utils.writeToHdfs(graph.getVertices(), "3_post_type_group_by");
//    out.addPreClusterSizes("3 cluster sizes post typegroupby", graph.getVertices(), Utils.HASH_CC);

    // TODO prepare is ok, perhaps delete property Constants.VERTEX_AGG_SIM_VALUE
    // TODO for alternative version, unneeded
    if (Constants.IS_SIMSORT_ENABLED) {
      graph = SimSort.execute(graph, 100);
    } else if (Constants.IS_SIMSORT_ALT_ENABLED){
      graph = SimSort.executeAlternative(graph, env);
    }
    graph = SimSort.excludeLowSimVertices(graph, env);

    /*
     * At this point, all edges within components are computed. Therefore we can delete links where
     * entities link several times to the same data source (e.g., geonames, linkedgeodata)
     * (remove 1:n links)
     */
    graph = Preprocessing.applyLinkFilterStrategy(graph, env, Boolean.TRUE);

    return graph;

  }
}
