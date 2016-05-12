package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.representative.MajorityPropertiesGroupReduceFunction;
import org.mappinganalysis.model.functions.simcomputation.AggSimValueTripletMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.functions.stats.FrequencyMapByFunction;
import org.mappinganalysis.utils.Utils;
import org.mappinganalysis.utils.functions.filter.OldHashCcFilterFunction;
import org.mappinganalysis.utils.functions.filter.RefineIdExcludeFilterFunction;
import org.mappinganalysis.utils.functions.filter.RefineIdFilterFunction;
import org.mappinganalysis.utils.functions.keyselector.OldHashCcKeySelector;

public class Refinement {
  private static final Logger LOG = Logger.getLogger(Refinement.class);

  /**
   * Prepare vertex dataset for the following refinement step
   * @param vertices representative vertices
   * @return prepared vertices
   */
  public static DataSet<Vertex<Long, ObjectMap>> init(DataSet<Vertex<Long, ObjectMap>> vertices) {
    DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets = vertices
        .filter(new OldHashCcFilterFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new TripletCreateGroupReduceFunction());

    return rejoinSingleVertexClustersFromSimSort(vertices, oldHashCcTriplets);
  }

  /**
     * Execute the refinement step - compare clusters with each other and combine similar clusters.
     * @param vertices prepared dataset
     * @return refined dataset
     */
  public static DataSet<Vertex<Long, ObjectMap>> execute(DataSet<Vertex<Long, ObjectMap>> vertices, ExampleOutput out)
      throws Exception {
    int maxClusterSize = 4;
    IterativeDataSet<Vertex<Long, ObjectMap>> workingSet = vertices.iterate(Integer.MAX_VALUE);

    DataSet<Vertex<Long, ObjectMap>> left = workingSet
        .filter(new ClusterSizeFilterFunction(maxClusterSize));
    left = printSuperstep(left);
    DataSet<Vertex<Long, ObjectMap>> right = workingSet
        .filter(new ClusterSizeFilterFunction(maxClusterSize));

    DataSet<Triplet<Long, ObjectMap, NullValue>> triplets = left.cross(right)
        .with(new TripletCreateCrossFunction())
        .filter(new EmptyTripletDeleteFilter());

    // - similarity on intriplets + threshold
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets = SimilarityComputation
        .computeSimilarities(triplets, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(Utils.MIN_SIM));

    // - exclude duplicate ontology vertices
    // - mark matches with more than 1 equal src/trg high similarity triplets
    similarTriplets = similarTriplets
        .leftOuterJoin(extractNoticableTriplets(similarTriplets))
        .where(0,1)
        .equalTo(0,1)
        .with(new ExcludeDuplicateOntologyTripletFlatJoinFunction());

    DataSet<Tuple4<Long, Long, Double, Integer>> noticableTuple4 = similarTriplets
        .filter(new RefineIdFilterFunction())
        .map(new CreateNoticableTripletMapFunction());

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> filteredNoticableTriplets = noticableTuple4
        .groupBy(0)
        .max(2).andSum(3)
        .union(noticableTuple4
            .groupBy(1)
            .max(2).andSum(3))
        .filter(new MaxSimPerNoticableTupleFilterFunction())
        .leftOuterJoin(similarTriplets)
        .where(0, 1)
        .equalTo(0, 1)
        .with(new TupleToTripletJoinFunction<Tuple4<Long, Long, Double, Integer>, ObjectMap>());

    DataSet<Vertex<Long, ObjectMap>> newClusters = similarTriplets
        .filter(new RefineIdExcludeFilterFunction()) // EXCLUDE_VERTEX_ACCUMULATOR counter
        .union(filteredNoticableTriplets)
        .map(new SimilarClusterMergeMapFunction()); // REFINEMENT_MERGE_ACCUMULATOR - new cluster count

    DataSet<Vertex<Long, ObjectMap>> newVertices = excludeClusteredVerticesFromInput(workingSet, similarTriplets);

    return workingSet.closeWith(newClusters.union(newVertices), newClusters);
  }

  private static DataSet<Vertex<Long, ObjectMap>> printSuperstep(DataSet<Vertex<Long, ObjectMap>> left) {
    DataSet<Vertex<Long, ObjectMap>> superstepPrinter = left
        .first(1)
        .filter(new RichFilterFunction<Vertex<Long, ObjectMap>>() {
          private Integer superstep = null;
          @Override
          public void open(Configuration parameters) throws Exception {
            this.superstep = getIterationRuntimeContext().getSuperstepNumber();
          }
          @Override
          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
            LOG.info("Superstep: " + superstep);
            return false;
          }
        });

    left = left.union(superstepPrinter);
    return left;
  }

  private static DataSet<Vertex<Long, ObjectMap>> excludeClusteredVerticesFromInput(
      DataSet<Vertex<Long, ObjectMap>> workingSet,
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets) {
    return similarTriplets
          .flatMap(new VertexExtractFlatMapFunction())
          .<Tuple1<Long>>project(0)
          .distinct()
          .rightOuterJoin(workingSet)
          .where(0).equalTo(0)
          .with(new ExcludeVertexFlatJoinFunction());
  }

  /**
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occures
   */
  private static DataSet<Tuple4<Long, Long, Long, Double>> extractNoticableTriplets(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets) throws Exception {

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> equalSourceVertex = getDuplicateTriplets(similarTriplets, 0);
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> equalTargetVertex = getDuplicateTriplets(similarTriplets, 1);

    return excludeTuples(equalSourceVertex, 1)
        .union(excludeTuples(equalTargetVertex, 0))
        .distinct();
  }

  /**
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occures
   * @param triplets input triplets
   * @param column 0 - source, 1 - target
   * @return tuples which should be excluded
   */
  private static DataSet<Tuple4<Long, Long, Long, Double>> excludeTuples(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> triplets, final int column) {
    return triplets
        .groupBy(1 - column)
        .reduceGroup(new CollectExcludeTuplesGroupReduceFunction(column));
  }


  /**
   * Return triplet data for certain vertex id's. TODO check
   * @param similarTriplets source triplets
   * @param column search for vertex id in triplets source (0) or target (1)
   * @return resulting triplets
   */
  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> getDuplicateTriplets(DataSet<Triplet<Long, ObjectMap,
      ObjectMap>> similarTriplets, int column) {

    DataSet<Tuple2<Long, Long>> duplicateTuples = similarTriplets
        .<Tuple2<Long, Long>>project(0,1)
        .map(new FrequencyMapByFunction(column))
        .groupBy(0)
        .sum(1)
        .filter(new FilterFunction<Tuple2<Long, Long>>() {
          @Override
          public boolean filter(Tuple2<Long, Long> tuple) throws Exception {
            LOG.info("getDuplicateTriplets: " + tuple.toString());
            return tuple.f1 > 1;
          }
        });

    return duplicateTuples.leftOuterJoin(similarTriplets)
        .where(0)
        .equalTo(column)
        .with(new JoinFunction<Tuple2<Long, Long>, Triplet<Long, ObjectMap, ObjectMap>,
            Triplet<Long, ObjectMap, ObjectMap>>() {
          @Override
          public Triplet<Long, ObjectMap, ObjectMap> join(Tuple2<Long, Long> tuple,
                                                          Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
            return triplet;
          }
        });

    // TODO check method
//    return similarTriplets.<Tuple2<Long, Long>>project(0, 1)
//        .groupBy(0)
//        .sum(1)
//        .duplicateTuples(new FilterFunction<Tuple2<Long, Long>>() {
//          @Override
//          public boolean duplicateTuples(Tuple2<Long, Long> tuple) throws Exception {
//            return tuple.f1 > 1;
//          }
//        })
//        .leftOuterJoin(similarTriplets)
//        .where(0)
//        .equalTo(column)
//        .with(new JoinFunction<Tuple2<Long, Long>, Triplet<Long, ObjectMap, ObjectMap>,
//            Triplet<Long, ObjectMap, ObjectMap>>() {
//          @Override
//          public Triplet<Long, ObjectMap, ObjectMap> join(Tuple2<Long, Long> tuple,
//              Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
//            return triplet;
//          }
//        });
  }

  /**
   * With SimSort, we extract (potentially two) single vertices from a component.
   * Here we try to rejoin vertices which have been in one cluster previously to reduce the
   * complexity for the following merge step.
   */
  private static DataSet<Vertex<Long, ObjectMap>> rejoinSingleVertexClustersFromSimSort(
      DataSet<Vertex<Long, ObjectMap>> representativeVertices,
      DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets) {

    // vertices with min sim, some triplets get omitted -> error cause
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newBaseTriplets = SimilarityComputation
        .computeSimilarities(oldHashCcTriplets, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES))
        .withForwardedFields("f0;f1;f2;f3");

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newRepresentativeTriplets = newBaseTriplets
        .filter(new MinRequirementThresholdFilterFunction(Utils.MIN_SIM));


    // reduce to single representative, some vertices are now missing
    DataSet<Vertex<Long, ObjectMap>> newRepresentativeVertices = newRepresentativeTriplets
        .flatMap(new VertexExtractFlatMapFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new MajorityPropertiesGroupReduceFunction());

    return newRepresentativeTriplets
        .flatMap(new VertexExtractFlatMapFunction())
        .<Tuple1<Long>>project(0)
        .distinct()
        .rightOuterJoin(representativeVertices)
        .where(0)
        .equalTo(0)
        .with(new ExcludeVertexFlatJoinFunction())
        .union(newRepresentativeVertices);
  }

  private static class CreateNoticableTripletMapFunction implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple4<Long, Long, Double, Integer>> {
    @Override
    public Tuple4<Long, Long, Double, Integer> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
      return new Tuple4<>(triplet.getSrcVertex().getId(),
          triplet.getTrgVertex().getId(),
          triplet.getEdge().getValue().getSimilarity(),
          1);
    }
  }

  private static class MaxSimPerNoticableTupleFilterFunction implements FilterFunction<Tuple4<Long, Long, Double, Integer>> {
    @Override
    public boolean filter(Tuple4<Long, Long, Double, Integer> tuple) throws Exception {
//            if (tuple.f3 > 1) {
//              LOG.info("filteredNoticable " + tuple.toString());
//            }
      return tuple.f3 > 1;
    }
  }

  private static class TupleToTripletJoinFunction<T, O>
      implements JoinFunction<T, Triplet<Long,ObjectMap,O>, Triplet<Long, ObjectMap, O>> {
    @Override
    public Triplet<Long, ObjectMap, O> join(T tuple, Triplet<Long, ObjectMap, O> triplet) throws Exception {
      return triplet;
    }
  }
}
