package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
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
import org.mappinganalysis.utils.functions.keyselector.RefineIdKeySelector;

public class Refinement {

  /**
   * Prepare vertex dataset for the following refinement step
   * @param vertices input vertices
   * @param minClusterSim minimum similarity for new clusters
   * @return prepared vertices
   */
  public static DataSet<Vertex<Long, ObjectMap>> init(DataSet<Vertex<Long, ObjectMap>> vertices, double minClusterSim) {
    DataSet<Triplet<Long, ObjectMap, NullValue>> sortedOutSimSortTriplets = vertices
        .filter(new OldHashCcFilterFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new TripletCreateGroupReduceFunction());

    return integrateMergedVertices(vertices, sortedOutSimSortTriplets, minClusterSim);
//    LOG.info("representativeVertices 2. count: " + representativeVertices.count());
  }

  /**
   * Execute the refinement step - compare clusters with each other and combine similar clusters.
   * @param vertices prepared dataset
   * @return refined dataset
   */
  public static DataSet<Vertex<Long, ObjectMap>> execute(DataSet<Vertex<Long, ObjectMap>> vertices) throws Exception {
    int maxClusterSize = 4;
    IterativeDataSet<Vertex<Long, ObjectMap>> loop = vertices.iterate(maxClusterSize);

    DataSet<Vertex<Long, ObjectMap>> left = loop
        .filter(new ClusterSizeFilterFunction());
    DataSet<Vertex<Long, ObjectMap>> right = loop
        .filter(new ClusterSizeFilterFunction(maxClusterSize));

    // TODO how to possibly avoid the cross?
    DataSet<Triplet<Long, ObjectMap, NullValue>> loopTriplets = left.cross(right)
        .with(new TripletCreateCrossFunction())
        .filter(new EmptyTripletDeleteFilter());

    // - similarity on intriplets + threshold
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets = SimilarityComputation
        .computeSimilarities(loopTriplets, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(0.7));

    // - exclude duplicate ontology vertices
    // - mark matches with more than 1 equal src/trg high similarity triplets
    similarTriplets = similarTriplets
        .leftOuterJoin(extractExcludeTriplets(similarTriplets))
        .where(0,1)
        .equalTo(0,1)
        .with(new ExcludeDuplicateOntologyTripletFlatJoinFunction());

    // - create cluster for marked triplets, e.g. (1, 2), (1, 3) merged to cluster vertex with vertex list (1,2,3)
    DataSet<Vertex<Long, ObjectMap>> partlyVertices = similarTriplets
        .filter(new RefineIdFilterFunction())
        .flatMap(new VertexExtractFlatMapFunction())
        .groupBy(new RefineIdKeySelector())
        .reduceGroup(new MajorityPropertiesGroupReduceFunction());

    DataSet<Vertex<Long, ObjectMap>> newClusters = similarTriplets
        .filter(new RefineIdExcludeFilterFunction()) // EXCLUDE_VERTEX_ACCUMULATOR counter
        .map(new SimilarClusterMergeMapFunction()) // REFINEMENT_MERGE_ACCUMULATOR - new cluster count
        .union(partlyVertices);

    DataSet<Vertex<Long, ObjectMap>> newVertices =  similarTriplets
        .flatMap(new VertexExtractFlatMapFunction())
        .<Tuple1<Long>>project(0)
        .distinct()
        .rightOuterJoin(vertices)
        .where(0).equalTo(0)
        .with(new ExcludeVertexFlatJoinFunction());

    return loop.closeWith(newClusters.union(newVertices));
  }


  /**
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occures
   */
  private static DataSet<Tuple3<Long, Long, Long>> extractExcludeTriplets(
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
  private static DataSet<Tuple3<Long, Long, Long>> excludeTuples(DataSet<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                                                                 final int column) {
    return triplets.groupBy(1 - column)
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

    DataSet<Tuple2<Long, Long>> foo = similarTriplets.project(0,1);

    DataSet<Tuple2<Long, Long>> map = foo.map(new FrequencyMapByFunction(column));

    DataSet<Tuple2<Long, Long>> filter = map.groupBy(0)
        .sum(1)
        .filter(new FilterFunction<Tuple2<Long, Long>>() {
          @Override
          public boolean filter(Tuple2<Long, Long> tuple) throws Exception {
            return tuple.f1 > 1;
          }
        });

    return filter.leftOuterJoin(similarTriplets)
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
//        .filter(new FilterFunction<Tuple2<Long, Long>>() {
//          @Override
//          public boolean filter(Tuple2<Long, Long> tuple) throws Exception {
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

  private static DataSet<Vertex<Long, ObjectMap>> integrateMergedVertices(
      DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices,
      DataSet<Triplet<Long, ObjectMap, NullValue>> sortedOutSimSortTriplets, double threshold) {

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newReprBaseTriplets = SimilarityComputation
        .computeSimilarities(sortedOutSimSortTriplets, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES)).withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(threshold));

    DataSet<Vertex<Long, ObjectMap>> newRepresentativeVertices = newReprBaseTriplets
        .flatMap(new VertexExtractFlatMapFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new MajorityPropertiesGroupReduceFunction());

    return newReprBaseTriplets
        .flatMap(new VertexExtractFlatMapFunction())
        .<Tuple1<Long>>project(0)
        .distinct()
        .rightOuterJoin(mergedClusterVertices)
        .where(0)
        .equalTo(0)
        .with(new ExcludeVertexFlatJoinFunction())
        .union(newRepresentativeVertices);
  }
}
