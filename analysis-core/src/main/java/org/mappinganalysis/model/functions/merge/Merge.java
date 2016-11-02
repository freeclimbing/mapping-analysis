package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.MajorityPropertiesGroupReduceFunction;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.AggSimValueTripletMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.functions.stats.FrequencyMapByFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.RightSideOnlyJoinFunction;
import org.mappinganalysis.util.functions.filter.OldHashCcFilterFunction;
import org.mappinganalysis.util.functions.keyselector.OldHashCcKeySelector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Merge process, iteratively collate similar clusters in compliance with restrictions
 * according to types and data sources.
 */
public class Merge {
  private static final Logger LOG = Logger.getLogger(Merge.class);

  /**
   * Prepare vertex dataset for the following refinement step
   * @param vertices representative vertices
   * @return prepared vertices
   */
  public static DataSet<Vertex<Long, ObjectMap>> init(DataSet<Vertex<Long, ObjectMap>> vertices,
                                                      ExampleOutput out) {
    DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets = vertices
        .filter(new OldHashCcFilterFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new TripletCreateGroupReduceFunction());

    return rejoinSingleVertexClustersFromSimSort(vertices, oldHashCcTriplets, out);
  }

  /**
   * Execute the refinement step - compare clusters with each other and combine similar clusters.
   * @param baseClusters prepared dataset
   * @return refined dataset
   */
  public static DataSet<Vertex<Long, ObjectMap>> execute(
      DataSet<Vertex<Long, ObjectMap>> baseClusters,
      int sourcesCount,
      ExampleOutput out,
      ExecutionEnvironment env)
      throws Exception {

    // todo remove triplets, at all!
    DataSet<Triplet<Long, ObjectMap, NullValue>> baseTriplets = createBaseTriplets(
        sourcesCount,
        baseClusters.filter(new ClusterSizeFilterFunction(sourcesCount)));

    // - similarity on intriplets + threshold
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets = SimilarityComputation
        .computeSimilarities(baseTriplets, Constants.SIM_GEO_LABEL_STRATEGY)
        .map(new AggSimValueTripletMapFunction(Constants.IGNORE_MISSING_PROPERTIES,
            Constants.MIN_LABEL_PRIORITY_SIM))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(Constants.MIN_CLUSTER_SIM));


    IterativeDataSet<Vertex<Long, ObjectMap>> workingSet = baseClusters.iterate(Integer.MAX_VALUE);

    DataSet<Vertex<Long, ObjectMap>> stepVertices = workingSet
        .filter(new ClusterSizeFilterFunction(sourcesCount));
    stepVertices = printSuperstep(stepVertices);


    // TODO restrict to only best value merging
    // FIXME: 11/1/16 hacky hack
    // simply use blocking keys to reduce computational effort
    ReduceOperator<Tuple3<Long, Long, Double>> tmp = similarTriplets
        .map(triplet -> new Tuple3<>(triplet.f0, triplet.f1, triplet.f4.getEdgeSimilarity()))
        .returns(new TypeHint<Tuple3<Long, Long, Double>>() {
        })
        .maxBy(2); // not safe to assume!

    similarTriplets = similarTriplets.join(tmp)
        .where(0,1)
        .equalTo(0,1)
        .with((triplet, tuple) -> triplet)
        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> newClusters = similarTriplets
        .map(new SimilarClusterMergeMapFunction());

    DataSet<Vertex<Long, ObjectMap>> newVertices = excludeClusteredVerticesFromInput(workingSet, similarTriplets);

    return workingSet.closeWith(newClusters
                                  .union(newVertices)
                                  .distinct(0),
                                newClusters);
  }

  // wtf?
  // currently executed AFTER sim comp
  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> restrictWithBlocking(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets) {
    // tuple mapper get blocking label key
    DataSet<Tuple4<Long, Long, String, Double>> blockingKeySimTuples = similarTriplets
        .map(triplet -> new Tuple4<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            Utils.getBlockingLabel(triplet.getSrcVertex().getValue().getLabel()),
            triplet.getEdge().getValue().getEdgeSimilarity()))
        .returns(new TypeHint<Tuple4<Long, Long, String, Double>>() {});

    DataSet<Tuple4<Long, Long, String, Double>> relevantTuples = blockingKeySimTuples
        .groupBy(2)
        .max(3).andMax(0).andMax(1)
        .leftOuterJoin(blockingKeySimTuples)
        .where(2, 3)
        .equalTo(2, 3)
        .with((first, second) -> second)
        .returns(new TypeHint<Tuple4<Long, Long, String, Double>>() {});

    similarTriplets = relevantTuples
        .leftOuterJoin(similarTriplets)
        .where(0, 1)
        .equalTo(0, 1)
        .with((left, right) -> right)
        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {});
    return similarTriplets;
  }

  /**
   * Process input vertices and create base triplets for comparison using the type/source restrictions
   * according the merge procedure
   */
  private static DataSet<Triplet<Long, ObjectMap, NullValue>> createBaseTriplets(
      int sourcesCount,
      DataSet<Vertex<Long, ObjectMap>> stepVertices) {

//    DataSet<Tuple2<Long, String>> blockedLabels = stepVertices
//        .map(vertex -> new Tuple2<>(
//            vertex.getId(),
//            Utils.getBlockingLabel(vertex.getValue().getLabel())))
//        .returns(new TypeHint<Tuple2<Long, String>>() {});
//
//    DataSet<Tuple2<Long, Long>> blockedTuples = blockedLabels
//        .groupBy(1)
//        .reduceGroup(new LabelBlockingGroupReduceFunction());

    return stepVertices
          .map(new AddShadingTypeMapFunction())
          .flatMap(new MergeTupleMapper(sourcesCount)) // create merge tuples, MANY results
          .groupBy(2) // group by label
          .reduceGroup(new BaseTripletCreateFunction(sourcesCount)) // little bit reduction, still MANY
          // at this point, blocking needs to be completed
          .join(stepVertices)
          .where(0)
          .equalTo(0)
          .with((triplet, vertex) -> {
            triplet.f2 = vertex.getValue();
            return triplet;
          })
          .returns(new TypeHint<Triplet<Long, ObjectMap, NullValue>>() {})
          .join(stepVertices)
          .where(1)
          .equalTo(0)
          .with((triplet, vertex) -> {
            triplet.f3 = vertex.getValue();  // ... even more inflation
            return triplet;
          })
          .returns(new TypeHint<Triplet<Long, ObjectMap, NullValue>>() {});
//          .distinct(0, 1);
  }

  /**
   * Helper method to write the current iteration superstep to the log.
   */
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

  /**
   * Remove cluster ids from vertex set which are merged.
   * @param baseVertexSet base vertex set
   * @param similarTriplets triplets where source and target vertex needs to be removed from base vertex set
   * @return cleaned base vertex set
   */
  private static DataSet<Vertex<Long, ObjectMap>> excludeClusteredVerticesFromInput(
      DataSet<Vertex<Long, ObjectMap>> baseVertexSet,
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets) {
    return similarTriplets
          .flatMap(new VertexExtractFlatMapFunction())
          .<Tuple1<Long>>project(0)
          .distinct()
          .rightOuterJoin(baseVertexSet)
          .where(0).equalTo(0)
          .with(new RightSideOnlyJoinFunction<>());
  }


  /**
   * Get the hash map value having the highest count of occurrence.
   * For label property, if count is equal, a longer string is preferred.
   * @param map containing value options with count of occurrence
   * @param propertyName if label, for same occurrence count the longer string is taken
   * @return resulting value
   */
  public static <T> T getFinalValue(HashMap<T, Integer> map, String propertyName) {
    Map.Entry<T, Integer> finalEntry = null;
    map = Utils.sortByValue(map);

    for (Map.Entry<T, Integer> entry : map.entrySet()) {
      if (finalEntry == null || Ints.compare(entry.getValue(), finalEntry.getValue()) > 0) {
        finalEntry = entry;
      } else if (entry.getKey() instanceof String
          && propertyName.equals(Constants.LABEL)
          && Ints.compare(entry.getValue(), finalEntry.getValue()) >= 0) {
        String labelKey = entry.getKey().toString();
        if (labelKey.length() > finalEntry.getKey().toString().length()) {
          finalEntry = entry;
        }
      }
    }

    checkArgument(finalEntry != null, "Entry must not be null");
    return finalEntry.getKey();
  }

  /**
   * With SimSort, we extract (potentially two) single vertices from a component.
   * Here we try to rejoin vertices which have been in one cluster previously to reduce the
   * complexity for the following merge step.
   */
  private static DataSet<Vertex<Long, ObjectMap>> rejoinSingleVertexClustersFromSimSort(
      DataSet<Vertex<Long, ObjectMap>> representativeVertices,
      DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets, ExampleOutput out) {

    // vertices with min sim, some triplets get omitted -> error cause
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newBaseTriplets = SimilarityComputation
        .computeSimilarities(oldHashCcTriplets, Constants.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Constants.IGNORE_MISSING_PROPERTIES,
            Constants.MIN_LABEL_PRIORITY_SIM))
        .withForwardedFields("f0;f1;f2;f3");

//    out.addDataSetCount("newBaseTriplets", newBaseTriplets);
//    Utils.writeToFile(newBaseTriplets, "6-init-newBaseTriplets");

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newRepresentativeTriplets = newBaseTriplets
        .filter(new MinRequirementThresholdFilterFunction(Constants.MIN_CLUSTER_SIM));

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
        .with(new RightSideOnlyJoinFunction<>())
        .union(newRepresentativeVertices);
  }

  /**
   * OLD MERGE
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occurs
   */
  @Deprecated
  private static DataSet<Tuple4<Long, Long, Long, Double>> extractNoticableTriplets(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets) throws Exception {

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> equalSourceVertex = getDuplicateTriplets(similarTriplets, 0);
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> equalTargetVertex = getDuplicateTriplets(similarTriplets, 1);

    return excludeTuples(equalSourceVertex, 1)
        .union(excludeTuples(equalTargetVertex, 0))
        .distinct();
  }

  /**
   * OLD MERGE
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occures
   *
   * return value for "to be excluded" triplets:
   * - srcId, trgId, Long.MIN_VALUE, 0D
   *
   * @param triplets input triplets
   * @param column 0 - source, 1 - target
   * @return tuples which should be excluded
   */
  @Deprecated
  private static DataSet<Tuple4<Long, Long, Long, Double>> excludeTuples(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> triplets, final int column) {
    return triplets
        .groupBy(1 - column)
        .reduceGroup(new CollectExcludeTuplesGroupReduceFunction(column));
  }


  /**
   * OLD MERGE
   * Return triplet data for certain vertex id's. TODO check
   * @param similarTriplets source triplets
   * @param column search for vertex id in triplets source (0) or target (1)
   * @return resulting triplets
   */
  @Deprecated
  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> getDuplicateTriplets(
      DataSet<Triplet<Long, ObjectMap,
      ObjectMap>> similarTriplets, int column) {

    DataSet<Tuple2<Long, Long>> duplicateTuples = similarTriplets
        .<Tuple2<Long, Long>>project(0,1)
        .map(new FrequencyMapByFunction(column))
        .groupBy(0)
        .sum(1)
        .filter(tuple -> {
          if (tuple.f1>1) LOG.info("getDuplicateTriplets: " + tuple.toString());
          return tuple.f1 > 1;
        });

    return duplicateTuples.leftOuterJoin(similarTriplets)
        .where(0)
        .equalTo(column)
        .with((tuple, triplet) -> triplet)
        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {});
  }

  /**
   * OLD MERGE
   * new type handling, most likely we dont need this anymore in merge, check
   */
  @Deprecated
  private static class TypeOverlapFlatMapFunction
      implements FlatMapFunction<Triplet<Long, ObjectMap, NullValue>,
      Triplet<Long, ObjectMap, NullValue>> {
    @Override
    public void flatMap(Triplet<Long, ObjectMap, NullValue> triplet,
                        Collector<Triplet<Long, ObjectMap, NullValue>> out) throws Exception {
      Set<String> srcTypes = triplet.getSrcVertex().getValue().getTypes(Constants.COMP_TYPE);
      Set<String> trgTypes = triplet.getTrgVertex().getValue().getTypes(Constants.COMP_TYPE);

      if (Utils.hasEmptyType(srcTypes, trgTypes)) {
//        LOG.info("missing type: " + triplet.f0 + " " + triplet.f1);
        out.collect(triplet);
      } else if (Doubles.compare(Utils.getTypeSim(srcTypes, trgTypes), 0) != 0) {
//        LOG.info("matching types: " + triplet.toString());
        out.collect(triplet);
      }
    }
  }

  private static class LabelBlockingGroupReduceFunction implements GroupReduceFunction<Tuple2<Long, String>, Tuple2<Long, Long>> {
    @Override
    public void reduce(Iterable<Tuple2<Long, String>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
      HashSet<Tuple2<Long, String>> leftSide = Sets.newHashSet(values);
      HashSet<Tuple2<Long, String>> rightSide = Sets.newHashSet(leftSide);

      Tuple2<Long, Long> result = new Tuple2<>();

      for (Tuple2<Long, String> leftVertex : leftSide) {
        result.f0 = leftVertex.f0;
        rightSide.remove(leftVertex);
        for (Tuple2<Long, String> rightVertex : rightSide) {
          result.f1 = rightVertex.f0;

          out.collect(result);
        }
      }
    }
  }
}
