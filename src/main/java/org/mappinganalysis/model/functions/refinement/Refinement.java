package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
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
  private static final Logger LOG = Logger.getLogger(Refinement.class);

  /**
   * Prepare vertex dataset for the following refinement step
   * @param vertices input vertices
   * @return prepared vertices
   */
  public static DataSet<Vertex<Long, ObjectMap>> init(DataSet<Vertex<Long, ObjectMap>> vertices) {
    DataSet<Triplet<Long, ObjectMap, NullValue>> sortedOutSimSortTriplets = vertices
        .filter(new OldHashCcFilterFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new TripletCreateGroupReduceFunction());

    return integrateMergedVertices(vertices, sortedOutSimSortTriplets);
  }


  /**
   * todo
   * @param vertices
   * @param leftSize smaller size
   * @param rightSize
   * @return
   * @throws Exception
   */
  public static DataSet<Vertex<Long, ObjectMap>> executeThird(
      DataSet<Vertex<Long, ObjectMap>> vertices, int leftSize, int rightSize, double minThreshold)
      throws Exception {
    DataSet<Vertex<Long, ObjectMap>> left = vertices
        .filter(new ClusterExactSizeFilterFunction(leftSize));
    DataSet<Vertex<Long, ObjectMap>> right = vertices
        .filter(new ClusterExactSizeFilterFunction(rightSize));

    DataSet<Triplet<Long, ObjectMap, NullValue>> triplets = left.cross(right)
        .with(new TripletCreateCrossFunction())
        .filter(new EmptyTripletDeleteFilter());

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets = SimilarityComputation
        .computeSimilarities(triplets, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(minThreshold));

    Utils.writeToHdfs(similarTriplets, "simTriplets"+leftSize+rightSize+minThreshold);

    DataSet<Tuple4<Long, Long, Long, Double>> noticableTriplets = extractExcludeTriplets(similarTriplets);
    Utils.writeToHdfs(noticableTriplets, "noticableTriplets"+leftSize+rightSize+minThreshold);

    similarTriplets = similarTriplets
        .leftOuterJoin(noticableTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new FlatJoinFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple4<Long, Long, Long, Double>,
            Triplet<Long, ObjectMap, ObjectMap>>() {
          @Override
          public void join(Triplet<Long, ObjectMap, ObjectMap> left, Tuple4<Long, Long, Long, Double> right, Collector<Triplet<Long, ObjectMap, ObjectMap>> collector) throws Exception {
            if (right == null) {
              collector.collect(left);
            }
          }
        });

//    noticableTriplets.
//        .with(new ExcludeDuplicateOntologyTripletFlatJoinFunction());

        DataSet<Vertex<Long, ObjectMap>> clustersMoreThanTwoSimTriplets = similarTriplets
        .filter(new RefineIdFilterFunction())
        .flatMap(new VertexExtractFlatMapFunction())
        .groupBy(new RefineIdKeySelector())
        .reduceGroup(new MajorityPropertiesGroupReduceFunction());

        DataSet<Vertex<Long, ObjectMap>> newClusters = similarTriplets
        .filter(new RefineIdExcludeFilterFunction()) // EXCLUDE_VERTEX_ACCUMULATOR counter
        .map(new SimilarClusterMergeMapFunction()) // REFINEMENT_MERGE_ACCUMULATOR - new cluster count
        .union(clustersMoreThanTwoSimTriplets);

//    Utils.writeToHdfs(newClusters, "newClusters"+left+right+minThreshold);


    DataSet<Vertex<Long, ObjectMap>> verticesNextStep
        = excludeClusteredVerticesFromInput(vertices, similarTriplets);

    return newClusters.union(verticesNextStep);
  }

  /**
     * Execute the refinement step - compare clusters with each other and combine similar clusters.
     * @param vertices prepared dataset
     * @return refined dataset
     */
  public static DataSet<Vertex<Long, ObjectMap>> execute(DataSet<Vertex<Long, ObjectMap>> vertices)
      throws Exception {
    int maxClusterSize = 4;
    IterativeDataSet<Vertex<Long, ObjectMap>> loop = vertices.iterate(maxClusterSize);

    DataSet<Vertex<Long, ObjectMap>> left = loop
        .filter(new ClusterSizeFilterFunction());
    DataSet<Vertex<Long, ObjectMap>> right = loop
        .filter(new ClusterSizeFilterFunction(maxClusterSize));

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

    DataSet<Vertex<Long, ObjectMap>> newVertices = excludeClusteredVerticesFromInput(vertices, similarTriplets);

    return loop.closeWith(newClusters.union(newVertices));
  }

  private static DataSet<Vertex<Long, ObjectMap>> excludeClusteredVerticesFromInput(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets) {
    return similarTriplets
          .flatMap(new VertexExtractFlatMapFunction())
          .<Tuple1<Long>>project(0)
          .distinct()
          .rightOuterJoin(vertices)
          .where(0).equalTo(0)
          .with(new ExcludeVertexFlatJoinFunction());
  }

  public static DataSet<Vertex<Long, ObjectMap>> executeAlternative(
      DataSet<Vertex<Long, ObjectMap>> vertices) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> left = vertices
        .filter(new ClusterExactSizeFilterFunction(1));
    DataSet<Vertex<Long, ObjectMap>> right = vertices
        .filter(new ClusterExactSizeFilterFunction(1));

    DataSet<Triplet<Long, ObjectMap, NullValue>> blockedCrossInput = getBlockedCrossInput(left, right);

    // - similarity on intriplets + threshold
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets = SimilarityComputation
        .computeSimilarities(blockedCrossInput, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(0.7));

//    Utils.writeToHdfs(similarTriplets, "similarTripletsFresh");

    // - exclude duplicate ontology vertices
    // - mark matches with more than 1 equal src/trg high similarity triplets
    similarTriplets = similarTriplets
        .leftOuterJoin(extractExcludeTriplets(similarTriplets))
        .where(0,1)
        .equalTo(0,1)
        .with(new ExcludeDuplicateOntologyTripletFlatJoinFunction());

//    Utils.writeToHdfs(similarTriplets, "similarTripletsExcludeDuplicate");

    // - create cluster for marked triplets, e.g. (1, 2), (1, 3) merged to cluster vertex with vertex list (1,2,3)
    DataSet<Vertex<Long, ObjectMap>> clustersMoreThanTwoSimTriplets = similarTriplets
        .filter(new RefineIdFilterFunction())
        .flatMap(new VertexExtractFlatMapFunction())
        .groupBy(new RefineIdKeySelector())
        .reduceGroup(new MajorityPropertiesGroupReduceFunction());

//    Utils.writeToHdfs(clustersMoreThanTwoSimTriplets, "partlyVertices");

    DataSet<Vertex<Long, ObjectMap>> newClusters = similarTriplets
        .filter(new RefineIdExcludeFilterFunction()) // EXCLUDE_VERTEX_ACCUMULATOR counter
        .map(new SimilarClusterMergeMapFunction()) // REFINEMENT_MERGE_ACCUMULATOR - new cluster count
        .union(clustersMoreThanTwoSimTriplets);

//    Utils.writeToHdfs(newClusters, "newClusters");

    DataSet<Vertex<Long, ObjectMap>> verticesNextStep
        = excludeClusteredVerticesFromInput(vertices, similarTriplets);

    return newClusters.union(verticesNextStep);
  }

  private static DataSet<Triplet<Long, ObjectMap, NullValue>> getBlockedCrossInput(
      DataSet<Vertex<Long, ObjectMap>> left, DataSet<Vertex<Long, ObjectMap>> right) {
    DataSet<Triplet<Long, ObjectMap, NullValue>> gnIn = left
        .filter(new SourceFilterFunction(Utils.GN_NS))
        .cross(right.filter(new NotSourceFilterFunction(Utils.GN_NS)))
        .with(new TripletCreateCrossFunction());

    DataSet<Triplet<Long, ObjectMap, NullValue>> nytIn = left
        .filter(new SourceFilterFunction(Utils.NYT_NS))
        .cross(right.filter(new NotSourceFilterFunction(Utils.NYT_NS)))
        .with(new TripletCreateCrossFunction());

    DataSet<Triplet<Long, ObjectMap, NullValue>> fbIn = left
        .filter(new SourceFilterFunction(Utils.FB_NS))
        .cross(right.filter(new NotSourceFilterFunction(Utils.FB_NS)))
        .with(new TripletCreateCrossFunction());

    DataSet<Triplet<Long, ObjectMap, NullValue>> lgdIn = left
        .filter(new SourceFilterFunction(Utils.LGD_NS))
        .cross(right.filter(new NotSourceFilterFunction(Utils.LGD_NS)))
        .with(new TripletCreateCrossFunction());

    DataSet<Triplet<Long, ObjectMap, NullValue>> dbpIn = left
        .filter(new SourceFilterFunction(Utils.DBP_NS))
        .cross(right.filter(new NotSourceFilterFunction(Utils.DBP_NS)))
        .with(new TripletCreateCrossFunction());

    return gnIn.union(nytIn).union(fbIn).union(lgdIn).union(dbpIn);
  }

  /**
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occures
   */
  private static DataSet<Tuple4<Long, Long, Long, Double>> extractExcludeTriplets(
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

  private static DataSet<Vertex<Long, ObjectMap>> integrateMergedVertices(
      DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices,
      DataSet<Triplet<Long, ObjectMap, NullValue>> sortedOutSimSortTriplets) {

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newReprBaseTriplets = SimilarityComputation
        .computeSimilarities(sortedOutSimSortTriplets, Utils.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Utils.IGNORE_MISSING_PROPERTIES))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(Utils.MIN_SIM));

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

  private static class ClusterExactSizeFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
    private final int maxSize;

    public ClusterExactSizeFilterFunction(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
      return vertex.getValue().getVerticesList().size() == maxSize;
    }
  }

  private static class NotSourceFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
    private final String ns;

    public NotSourceFilterFunction(String ns) {
      this.ns = ns;
    }

    @Override
    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
      return !vertex.getValue().getOntologiesList().contains(ns);
    }
  }

  private static class SourceFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
    private final String ns;

    public SourceFilterFunction(String ns) {
      this.ns = ns;
    }

    @Override
    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
      return vertex.getValue().getOntologiesList().contains(ns);
    }
  }
}
