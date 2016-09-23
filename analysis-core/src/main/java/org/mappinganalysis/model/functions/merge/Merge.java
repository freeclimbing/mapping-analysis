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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;
import org.mappinganalysis.model.functions.decomposition.representative.MajorityPropertiesGroupReduceFunction;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.AggSimValueTripletMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.functions.stats.FrequencyMapByFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.SourcesUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.RightSideOnlyJoinFunction;
import org.mappinganalysis.util.functions.filter.OldHashCcFilterFunction;
import org.mappinganalysis.util.functions.filter.RefineIdExcludeFilterFunction;
import org.mappinganalysis.util.functions.filter.RefineIdFilterFunction;
import org.mappinganalysis.util.functions.keyselector.OldHashCcKeySelector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * previously Refinement
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
     * @param env
   * @return refined dataset
     */
  public static DataSet<Vertex<Long, ObjectMap>> execute(
      DataSet<Vertex<Long, ObjectMap>> baseClusters,
      int sourcesCount,
      ExampleOutput out,
      ExecutionEnvironment env)
      throws Exception {

    IterativeDataSet<Vertex<Long, ObjectMap>> workingSet = baseClusters.iterate(Integer.MAX_VALUE);

    DataSet<Vertex<Long, ObjectMap>> stepVertices = workingSet
        .filter(new ClusterSizeFilterFunction(sourcesCount))
        .map(value -> {
          if (value.getId() == 1981L || value.getId() == 1982L || value.getId() == 6166L || value.getId() == 5488L) {
            LOG.info("###wei: " + value);
          }
          return value;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
    stepVertices = printSuperstep(stepVertices);

    DataSet<Triplet<Long, ObjectMap, NullValue>> baseTriplets1 = stepVertices
        .map(new AddShadingTypeMapFunction())
        .flatMap(new MergeTupleMapper()) // todo add/remove type asap
        .groupBy(2)
        .reduceGroup(new BaseTripletCreateFunction(sourcesCount));

    DataSet<Triplet<Long, ObjectMap, NullValue>> baseTriplets2 = baseTriplets1
        .leftOuterJoin(stepVertices)
        .where(0)
        .equalTo(0)
        .with(new AdvancedTripletCreateFunction(0));

    DataSet<Triplet<Long, ObjectMap, NullValue>> baseTriplets = baseTriplets2
        .leftOuterJoin(stepVertices)
        .where(1)
        .equalTo(0)
        .with(new AdvancedTripletCreateFunction(1))
        .distinct(0, 1)
        .flatMap(new TypeOverlapFlatMapFunction());

    // todo 1. sim computation
    // todo 2. optimize sim comp
    // todo 3. remove duplicate merge results

//    DataSet<Triplet<Long, ObjectMap, NullValue>> triplets = stepVertices.cross(stepVertices)
//        .with(new TripletCreateCrossFunction(Constants.SOURCE_COUNT))
//        .filter(value -> value.getSrcVertex().getId() != 0L && value.getTrgVertex().getId() != 0L);

    // - similarity on intriplets + threshold
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> similarTriplets = SimilarityComputation
        .computeSimilarities(baseTriplets, Constants.SIM_GEO_LABEL_STRATEGY)
        .map(new AggSimValueTripletMapFunction(Constants.IGNORE_MISSING_PROPERTIES,
            Constants.MIN_LABEL_PRIORITY_SIM))
        .withForwardedFields("f0;f1;f2;f3")
        .filter(new MinRequirementThresholdFilterFunction(Constants.MIN_CLUSTER_SIM));

//    // get entities occurring in several triplets
//    DataSet<Tuple2<Long, Integer>> duplicates = similarTriplets.<Tuple1<Long>>project(0)
//        .union(similarTriplets.<Tuple1<Long>>project(1))
//        .map(value -> new Tuple2<>(value.f0, 1))
//        .returns(new TypeHint<Tuple2<Long, Integer>>() {
//        })
//        .groupBy(0)
//        .sum(1)
//        .filter(tuple -> {
//          if (tuple.f1 > 1) {
//            LOG.info("duplicate count " + tuple.toString());
//          }
//          return tuple.f1 > 1;
//        });
//
//    // get problem triplets
//    similarTriplets = similarTriplets.leftOuterJoin(duplicates)
//        .where(0)
//        .equalTo(0)
//        .with(new LeftSideOnlyJoinFunction<>())
//        .union(similarTriplets.leftOuterJoin(duplicates)
//            .where(1)
//            .equalTo(0)
//            .with(new LeftSideOnlyJoinFunction<>()))
//        .distinct(0,1)
//        .map(value -> {
//          LOG.info("problem triplet: " + value.toString());
//          return value;
//        })
//        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {});


    // TODO this is OLD remove duplicate code
    // - exclude duplicate ontology vertices
    // - mark matches with more than 1 equal src/trg triplets


    // tuple mapper get blcoking label key
    DataSet<Tuple4<Long, Long, String, Double>> blockingKeySimTuples = similarTriplets
        .map(triplet -> {
          Tuple4<Long, Long, String, Double> tuple4 = new Tuple4<>(
              triplet.getSrcVertex().getId(),
              triplet.getTrgVertex().getId(),
              Utils.getBlockingLabel(triplet.getSrcVertex().getValue().getLabel()),
              triplet.getEdge().getValue().getEdgeSimilarity());
          LOG.info(tuple4.toString());
          return tuple4;
        })
        .returns(new TypeHint<Tuple4<Long, Long, String, Double>>() {});

    DataSet<Tuple4<Long, Long, String, Double>> relevantTuples = blockingKeySimTuples
        .groupBy(2)
        .max(3).andMax(0).andMax(1)
        .leftOuterJoin(blockingKeySimTuples)
        .where(2, 3)
        .equalTo(2, 3)
        .with((first, second) -> {
          LOG.info(first.toString() + " second: " + second.toString());
          if (second.f0 == 1981L || second.f0 == 1982L) {
            LOG.info("###wei " + second.toString());
          }
          return second;
        })
        .returns(new TypeHint<Tuple4<Long, Long, String, Double>>() {});

    similarTriplets = relevantTuples
        .leftOuterJoin(similarTriplets)
        .where(0, 1)
        .equalTo(0, 1)
        .with((left, right) -> {
          LOG.info("relevant: " + right.toString());
          if (right.getSrcVertex().getId() == 1981L || right.getSrcVertex().getId() == 1982L) {
            LOG.info("###wei " + right.toString());
          }
          return right;
        })
        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {
        });

//    similarTriplets = similarTriplets
//        .leftOuterJoin(extractNoticableTriplets(similarTriplets))
//        .where(0,1)
//        .equalTo(0,1)
//        .with(new ExcludeDuplicateOntologyTripletFlatJoinFunction())
//        .distinct(0,1);

//    DataSet<Tuple4<Long, Long, Double, Integer>> srcTrgSimOneTuple = similarTriplets
//        .filter(new RefineIdFilterFunction())
//        .map(new CreateNoticableTripletMapFunction()); // src, trg, sim, 1

//    DataSet<Triplet<Long, ObjectMap, ObjectMap>> filteredNoticableTriplets = srcTrgSimOneTuple
//        .groupBy(0)
//        .max(2).andSum(3)
//        .union(srcTrgSimOneTuple
//            .groupBy(1)
//            .max(2).andSum(3))
//        .filter(tuple -> tuple.f3 > 1)
//        .leftOuterJoin(similarTriplets)
//        .where(0, 1)
//        .equalTo(0, 1)
//        .with((tuple, triplet) -> {
//          LOG.info("final triplet: " + triplet.toString());
//          return triplet;
//        })
//        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> newClusters = similarTriplets
//        .filter(new RefineIdExcludeFilterFunction()) // EXCLUDE_VERTEX_ACCUMULATOR counter
//        .union(filteredNoticableTriplets)
        .map(new SimilarClusterMergeMapFunction());

    DataSet<Vertex<Long, ObjectMap>> newVertices = excludeClusteredVerticesFromInput(workingSet, similarTriplets);

    return workingSet.closeWith(newClusters
                                  .union(newVertices)
                                  .distinct(0),
                                newClusters);
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
   * Exclude
   * 1. tuples where duplicate ontologies are found
   * 2. tuples where more than one match occurs
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
   *
   * return value for "to be excluded" triplets:
   * - srcId, trgId, Long.MIN_VALUE, 0D
   *
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

  private static class BaseTripletCreateFunction
      implements GroupReduceFunction<MergeTuple, Triplet<Long, ObjectMap, NullValue>> {
    private final Triplet<Long, ObjectMap, NullValue> reuseTriplet;
    private final int sourcesCount;

    public BaseTripletCreateFunction(int sourcesCount) {
      this.sourcesCount = sourcesCount;
      this.reuseTriplet = new Triplet<>();
    }

    @Override
    public void reduce(Iterable<MergeTuple> values,
                       Collector<Triplet<Long, ObjectMap, NullValue>> out) throws Exception {
      HashSet<MergeTuple> leftSide = Sets.newHashSet(values);
      HashSet<MergeTuple> rightSide = Sets.newHashSet(leftSide);

      boolean isFirst = true;

      for (MergeTuple leftTuple : leftSide) {
        if (isFirst) {
          LOG.info("check group: " + leftTuple.toString());
          isFirst = false;
        }
        // todo type handling
        Integer leftSources = leftTuple.getIntSources();
        reuseTriplet.f0 = leftTuple.getVertexId();

        if (leftTuple.getVertexId() == 1981L || leftTuple.getVertexId() == 1982L) {
          LOG.info("###wei BSC left" + leftTuple.toString());
        }

        rightSide.remove(leftTuple);
        for (MergeTuple rightTuple : rightSide) {
          if (rightTuple.getVertexId() == 1981L
              || rightTuple.getVertexId() == 6166L
              || rightTuple.getVertexId() == 5488L) {
            LOG.info("###wei BSC right" + rightTuple.toString());
          }

          int summedSources = SourcesUtils.getSourceCount(leftSources)
              + SourcesUtils.getSourceCount(rightTuple.getIntSources());
          if (summedSources <= sourcesCount
              && !SourcesUtils.hasOverlap(leftSources, rightTuple.getIntSources())) {
            reuseTriplet.f1 = rightTuple.getVertexId();
            reuseTriplet.f4 = NullValue.getInstance();
            LOG.info("###weibaseTriplet: " + reuseTriplet.toString());

            out.collect(reuseTriplet);
          }
        }
      }
    }
  }

  /**
   * We should not use reuseTriplet here because we have to walk twice over the triplet
   */
  private static class AdvancedTripletCreateFunction
      implements JoinFunction<Triplet<Long, ObjectMap, NullValue>,
      Vertex<Long,ObjectMap>,
      Triplet<Long, ObjectMap, NullValue>> {
    private final int side;

    public AdvancedTripletCreateFunction(int side) {
      this.side = side;
    }
    @Override
    public Triplet<Long, ObjectMap, NullValue> join(Triplet<Long, ObjectMap, NullValue> left,
                                                    Vertex<Long, ObjectMap> right) throws Exception {
//      if (left.getSrcVertex().getId() == 1981L) {
//        LOG.info("###wei ATC triplet" + left.toString() + " side " + side);
        if (right == null) {
          LOG.info("###wei ATC " + left.getSrcVertex().getId() + " "
              + left.getTrgVertex().getId() + " " + side + " right null");
        }
//      }
//      LOG.info("###abc: " + left.toString() + " side " + side);
//      LOG.info("###abc2: " + right.toString() + " side " + side);
      if (side == 0) {
        left.f2 = right.getValue();
        left.f4 = NullValue.getInstance();
      } else if (side == 1) {
        left.f3 = right.getValue();
      }

      return left;
    }
  }

  private static class TypeOverlapFlatMapFunction
      implements FlatMapFunction<Triplet<Long, ObjectMap, NullValue>,
      Triplet<Long, ObjectMap, NullValue>> {
    @Override
    public void flatMap(Triplet<Long, ObjectMap, NullValue> triplet,
                        Collector<Triplet<Long, ObjectMap, NullValue>> out) throws Exception {
      Set<String> srcTypes = triplet.getSrcVertex().getValue().getTypes(Constants.COMP_TYPE);
      Set<String> trgTypes = triplet.getTrgVertex().getValue().getTypes(Constants.COMP_TYPE);

      if (!Utils.hasNoEmptyType(srcTypes, trgTypes)) {
//        LOG.info("missing type: " + triplet.f0 + " " + triplet.f1);
        out.collect(triplet);
      } else if (Doubles.compare(Utils.getTypeSim(srcTypes, trgTypes), 0) != 0) {
//        LOG.info("matching types: " + triplet.toString());
        out.collect(triplet);
      }
    }
  }
}
