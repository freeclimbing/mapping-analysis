package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.FullOuterJoinSimilarityValueFunction;
import org.mappinganalysis.model.functions.simsort.SimSort;
import org.mappinganalysis.model.functions.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.functions.typegroupby.TypeGroupBy;
import org.mappinganalysis.utils.Utils;

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
  public static DataSet<Edge<Long, ObjectMap>> computeEdgeSimFromGraph(Graph<Long, ObjectMap, NullValue> graph) {
    LOG.info("Compute Edge similarities based on vertex values, ignore missing properties: "
        + Utils.IGNORE_MISSING_PROPERTIES);

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

  /**
   * Compose similarity values based on existence: if property is missing, its not considered at all.
   * @param value property map
   * @return mean similarity value
   */
  public static double getMeanSimilarity(ObjectMap value) {
    double aggregatedSim = 0;
    int propCount = 0;
    if (value.containsKey(Utils.SIM_TRIGRAM)) {
      ++propCount;
      aggregatedSim = (double) value.get(Utils.SIM_TRIGRAM);
    }
    if (value.containsKey(Utils.SIM_TYPE)) {
      ++propCount;
      aggregatedSim += (double) value.get(Utils.SIM_TYPE);
    }
    if (value.containsKey(Utils.SIM_DISTANCE)) {
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
    if (values.containsKey(Utils.SIM_TRIGRAM)) {
      aggregatedSim = trigramWeight * (double) values.get(Utils.SIM_TRIGRAM);
    } else {
      aggregatedSim = 0;
    }
    if (values.containsKey(Utils.SIM_TYPE)) {
      aggregatedSim += typeWeight * (double) values.get(Utils.SIM_TYPE);
    }
    if (values.containsKey(Utils.SIM_DISTANCE)) {
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
    Object object = value.get(Utils.SIM_DISTANCE);
    if (object instanceof Double) {
      return (Double) object;
    } else {
      LOG.info("Error (should not occur)" + object.getClass().toString());
      return -1;
    }
    //      aggregatedSim += geoWeight * (Double) value.get(Utils.SIM_DISTANCE); // TODO why is this not working?
  }

  /**
   * Create the graph needed for any further computation. Compute similarities on edges.
   * @param graph preprocessed graph
   * @param env env
   * @return result
   */
  public static Graph<Long, ObjectMap, ObjectMap> initSimilarity(Graph<Long, ObjectMap, NullValue> graph,
                                                                 ExecutionEnvironment env) {
    return Graph.fromDataSet(
        graph.getVertices(),
        computeEdgeSimFromGraph(graph),
        env);
  }

  /**
   * Executes TypeGroupBy and SimSort method and returns the resulting graph.
   * @param graph graph with similarities already computed
   * @param processingMode if 'default', both methods are executed
   * @param minClusterSim minimal aggregated similarty to create a cluster
   * @param env env  @return result
   */
  public static Graph<Long, ObjectMap, ObjectMap> executeAdvanced(Graph<Long, ObjectMap, ObjectMap> graph,
                                                                  String processingMode, double minClusterSim,
                                                                  ExecutionEnvironment env) throws Exception {

    // TypeGroupBy
    // internally compType is used, afterwards typeIntern is used again
    graph = new TypeGroupBy().execute(graph, processingMode, 1000);

//    out.addVertexAndEdgeSizes("pre-simsort-prepare", graph);
    /* SimSort */
    graph = SimSort.prepare(graph, processingMode, env);


//    DataSet<Vertex<Long, String>> run = graph.mapVertices(new MapFunction<Vertex<Long, ObjectMap>, String>() {
//      @Override
//      public String map(Vertex<Long, ObjectMap> vertex) throws Exception {
//        return vertex.getValue().get(Utils.LABEL).toString();
//      }
//    }).run(new LabelPropagation<Long, String, ObjectMap>(10));
//
//    run.print();

//    Utils.writeToHdfs(graph.getVertices(), "simsortprepareVerts");

//    out.addVertexAndEdgeSizes("post-simsort-prepare", graph);

    graph = SimSort.execute(graph, 100, minClusterSim);

    return SimSort.excludeLowSimVertices(graph, env);

  }
}
