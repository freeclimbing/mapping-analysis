package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.FullOuterJoinSimilarityValueFunction;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

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
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> executeAdvanced(Graph<Long, ObjectMap, ObjectMap> graph,
                                                                  String processingMode, ExecutionEnvironment env,
                                                                  ExampleOutput out) throws Exception {
    if (Constants.PROC_MODE.equals(Constants.PREPROC)) {
      LOG.setLevel(Level.DEBUG);
     /*
      * sync begin
      */
      DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices().filter(value -> true);
      DataSet<Edge<Long, ObjectMap>> edges = graph.getEdges().filter(value -> true);
      graph = Graph.fromDataSet(vertices, edges, env);
     /*
      * sync end
      */

      graph = computeTransitiveClosureEdgeSimilarities(graph, env);

      graph = removeOneToManyVertices(graph, env);

//      graph = GraphUtils.addCcIdsToGraph(graph, env);
//      out.addVertexAndEdgeSizes("pre clustering", inputGraph);
      out.addPreClusterSizes("2 intial cluster sizes", graph.getVertices(), Constants.CC_ID);

      graph = TypeGroupBy.execute(graph, env, out);

      /*
      * SimSort (and postprocessing TypeGroupBy in prepare)
      */
      graph = SimSort.prepare(graph, env, out);

      out.addPreClusterSizes("3 cluster sizes post typegroupby", graph.getVertices(), Constants.HASH_CC);
      String outName = Constants.LL_MODE + "PreprocGraph";
      Utils.writeGraphToJSONFile(graph, outName);
      out.print();
    }
    return graph;

  }

  /**
   * After simple 1:n eliminating, still 1:n can reoccur after creating transitive closure
   * in components. The best candidate of the 1:n vertices remains in the vertex dataset,
   * others are removed.
   */
  public static Graph<Long, ObjectMap, ObjectMap> removeOneToManyVertices(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) {
    DataSet<Tuple3<Long, String, Double>> oneToManyCandidates = graph
        .groupReduceOnNeighbors(
        new NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
            Tuple3<Long, String, Double>>() {
          @Override
          public void iterateNeighbors(
              Vertex<Long, ObjectMap> vertex,
              Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighborEdgeVertices,
              Collector<Tuple3<Long, String, Double>> out) throws Exception {
            String ontology = vertex.getValue().getOntology();
            int neighborCount = 0;
            double vertexAggSim = 0d;
            boolean isRelevant = false;

            for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> edgeVertex : neighborEdgeVertices) {
              Edge<Long, ObjectMap> edge = edgeVertex.f0;
              Vertex<Long, ObjectMap> neighbor = edgeVertex.f1;
              ++neighborCount;
              if (!isRelevant && neighbor.getValue().getOntology().equals(ontology)) {
                isRelevant = true;
              }
              vertexAggSim += edge.getValue().getEdgeSimilarity();
            }

            if (isRelevant) {
              vertexAggSim /= neighborCount;
              out.collect(new Tuple3<>(vertex.getId(), ontology, vertexAggSim));
            }
          }
        }, EdgeDirection.ALL);

    DataSet<Vertex<Long, ObjectMap>> bestCandidates = oneToManyCandidates.groupBy(1)
        .max(2).andMin(0)
        .map(tuple -> new Tuple2<>(tuple.f1, tuple.f2)) // string double
        .returns(new TypeHint<Tuple2<String, Double>>() {
        })
        .leftOuterJoin(oneToManyCandidates)
        .where(0, 1)
        .equalTo(1, 2)
        .with(new FlatJoinFunction<Tuple2<String, Double>,
            Tuple3<Long, String, Double>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<String, Double> left,
                           Tuple3<Long, String, Double> right,
                           Collector<Tuple1<Long>> out) throws Exception {
            if (left != null) {
              out.collect(new Tuple1<>(right.f0));
            }
          }
        })
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple1<Long>,
            Vertex<Long, ObjectMap>,
            Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Tuple1<Long> left,
                           Vertex<Long, ObjectMap> right,
                           Collector<Vertex<Long, ObjectMap>> out) throws Exception {
            if (left != null) {
              LOG.info("vertex best candidate: " + right.toString());
              out.collect(right);
            }
          }
        });

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices()
        .leftOuterJoin(oneToManyCandidates)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Vertex<Long, ObjectMap>,
            Tuple3<Long, String, Double>,
            Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Vertex<Long, ObjectMap> left,
                           Tuple3<Long, String, Double> right,
                           Collector<Vertex<Long, ObjectMap>> out) throws Exception {
            if (right == null) {
              out.collect(left);
            }
          }
        })
        .union(bestCandidates);

    DataSet<Edge<Long, ObjectMap>> resultEdges = Preprocessing.deleteEdgesWithoutSourceOrTarget(
        graph.getEdges(),
        resultVertices);

    return Graph.fromDataSet(resultVertices, resultEdges, env);
  }

  public static Graph<Long, ObjectMap, ObjectMap> computeTransitiveClosureEdgeSimilarities(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) {

    final DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
        .getTransitiveClosureEdges(graph.getVertices(), new CcIdKeySelector());
    final DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
        .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
            Constants.SIM_GEO_LABEL_STRATEGY);
    graph = Graph.fromDataSet(graph.getVertices(), simEdges, env);
    return graph;
  }

  private static class LinkFilterExcludeEdgeFlatJoinFunction extends RichFlatJoinFunction<Edge<Long,ObjectMap>,
      Tuple1<Long>, Edge<Long, ObjectMap>> {
    private LongCounter filteredLinks = new LongCounter();

    @Override
    public void open(final Configuration parameters) throws Exception {
      super.open(parameters);
      getRuntimeContext().addAccumulator(Constants.PREPROC_LINK_FILTER_ACCUMULATOR, filteredLinks);
    }

    @Override
    public void join(Edge<Long, ObjectMap> left, Tuple1<Long> right,
                     Collector<Edge<Long, ObjectMap>> collector) throws Exception {
      if (right == null) {
        collector.collect(left);
      } else {
        filteredLinks.add(1L);
      }
    }
  }
}
