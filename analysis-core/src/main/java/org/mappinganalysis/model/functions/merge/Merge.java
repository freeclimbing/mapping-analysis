package org.mappinganalysis.model.functions.merge;

import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.MajorityPropertiesGroupReduceFunction;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.AggSimValueTripletMapFunction;
import org.mappinganalysis.model.functions.simcomputation.EdgeSimilarityFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.RightMinusLeftSideJoinFunction;
import org.mappinganalysis.util.functions.filter.OldHashCcFilterFunction;
import org.mappinganalysis.util.functions.keyselector.OldHashCcKeySelector;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

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
  // todo check SOURCE_COUNT
  public static DataSet<Vertex<Long, ObjectMap>> execute(
      DataSet<Vertex<Long, ObjectMap>> baseClusters,
      int sourcesCount)
      throws Exception {

    MergeTripletGeoLabelSimilarity simFunction =
        new MergeTripletGeoLabelSimilarity(new MeanAggregationMode());

    SimilarityComputation<MergeTriplet, MergeTriplet> similarityComputation = new SimilarityComputation
        .SimilarityComputationBuilder<MergeTriplet, MergeTriplet>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.5)
        .build();

    // initial solution set
    DataSet<MergeTuple> clusters = baseClusters
        .map(new AddShadingTypeMapFunction())
        .map(new MergeTupleCreator());

    // initial working set
    DataSet<MergeTriplet> initialWorkingSet = clusters
        .filter(tuple -> AbstractionUtils.getSourceCount(tuple.getIntSources()) < sourcesCount)
        .groupBy(7)
        .reduceGroup(new MergeTripletCreator(sourcesCount))
        .runOperation(similarityComputation);

    // initialize the iteration
    DeltaIteration<MergeTuple, MergeTriplet> iteration = clusters
        .iterateDelta(initialWorkingSet, 1000, 0);

    // log superstep
//    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
//        .map(x->x); // why do we need this line, not working without

    // start step function
    DataSet<MergeTriplet> workset = iteration.getWorkset();
    DataSet<MergeTriplet> maxTriplets = getIterationMaxTriplets(workset);
    DataSet<MergeTuple> delta = maxTriplets
        .flatMap(new MergeMapFunction());

    // remove max triplets from workset, they are getting merged anyway
    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    DataSet<Tuple2<Long, Long>> transitions = getTransitionElements(maxTriplets);

    DataSet<MergeTriplet> changes = getChangedTriplets(
        workset,
        delta,
        transitions);
    DataSet<MergeTriplet> changesWithNewSimilarities = computeSimilarities(
        sourcesCount,
        similarityComputation,
        changes);

    // throw out everything with transition elements
    DataSet<MergeTriplet> nonChangedWorksetPart = workset.leftOuterJoin(transitions)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(transitions)
        .where(1)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>());

    workset = nonChangedWorksetPart.union(changesWithNewSimilarities);

    return iteration.closeWith(delta, workset)
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeVertexCreator());
  }

  /**
   * Compute similarities within an iteration step and check side conditions
   * @param sourcesCount count of different data sources
   * @param similarityComputation type of similarity computation
   * @param changes input values
   * @return changed input values
   */
  private static DataSet<MergeTriplet> computeSimilarities(
      int sourcesCount, SimilarityComputation<MergeTriplet, MergeTriplet> similarityComputation, DataSet<MergeTriplet> changes) {
    return changes
        .map(triplet -> {
          if (triplet.getSrcId() > triplet.getTrgId()) {
            MergeTuple tmp = triplet.getSrcTuple();
            triplet.setSrcId(triplet.getTrgId());
            triplet.setSrcTuple(triplet.getTrgTuple());
            triplet.setTrgId(tmp.getId());
            triplet.setTrgTuple(tmp);
          }
          return triplet;
        })
        .distinct(0,1) // is needed
        .filter(triplet -> {
//          LOG.info("CHANGED AND GETS NEW SIM " + triplet.toString());
          boolean hasSourceOverlap = AbstractionUtils.hasOverlap(
              triplet.getSrcTuple().getIntSources(),
              triplet.getTrgTuple().getIntSources());
          boolean isSourceCountChecked = sourcesCount >=
              (AbstractionUtils.getSourceCount(triplet.getSrcTuple().getIntSources())
                  + AbstractionUtils.getSourceCount(triplet.getTrgTuple().getIntSources()));
          return !hasSourceOverlap && isSourceCountChecked;
        })
        .runOperation(similarityComputation);
  }

  private static FlatMapOperator<MergeTriplet, Tuple2<Long, Long>> getTransitionElements(
      DataSet<MergeTriplet> maxTriplets) {
    return maxTriplets
        .flatMap(new FlatMapFunction<MergeTriplet, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(MergeTriplet triplet, Collector<Tuple2<Long, Long>> out)
              throws Exception {
            Long min = triplet.getSrcId() < triplet.getTrgId()
                ? triplet.getSrcId() : triplet.getTrgId();
//            LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + min);
            out.collect(new Tuple2<>(triplet.getSrcId(), min));
            out.collect(new Tuple2<>(triplet.getTrgId(), min));
          }
        });
  }

  private static DataSet<MergeTriplet> getChangedTriplets(
      DataSet<MergeTriplet> workset,
      DataSet<MergeTuple> delta,
      DataSet<Tuple2<Long, Long>> transitions) {
    DataSet<MergeTriplet> leftChanges = workset.join(transitions)
        .where(0)
        .equalTo(0)
        .with((triplet, transition) -> {
          triplet.setSrcId(transition.f1);
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {})
        .distinct(0,1)
        .join(delta.filter(MergeTuple::isActive))
        .where(0)
        .equalTo(0)
        .with((triplet, newTuple) -> {
          triplet.setSrcTuple(newTuple);
//          LOG.info("LEFT DELTA JOIN " + triplet.toString());
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {});

    return workset.join(transitions)
        .where(1)
        .equalTo(0)
        .with((triplet, transition) -> {
          triplet.setTrgId(transition.f1);
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {})
        .distinct(0,1)
        .join(delta.filter(MergeTuple::isActive))
        .where(1)
        .equalTo(0)
        .with((triplet, newTuple) -> {
          triplet.setTrgTuple(newTuple);
//          LOG.info("RIGHT DELTA JOIN " + triplet.toString());
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {})
        .union(leftChanges);
  }

  private static DataSet<MergeTriplet> getIterationMaxTriplets(DataSet<MergeTriplet> workset) {
    DataSet<Tuple2<Double, String>> maxFilter = workset
        .groupBy(5)
        .max(4)
        .map(triplet -> new Tuple2<>(triplet.getSimilarity(),
            triplet.getSrcTuple().getBlockingLabel()))
        .returns(new TypeHint<Tuple2<Double, String>>() {})
        .distinct();

    return workset.join(maxFilter)
        .where(4)
        .equalTo(0)
        .with((triplet, tuple) ->  triplet)
        .returns(new TypeHint<MergeTriplet>() {})
        .distinct(0,1)
        .groupBy(5)
        .maxBy(4)
        .returns(new TypeHint<MergeTriplet>() {});
  }

  /**
   * Helper method to write the current iteration superstep to the log.
   */
  private static DataSet<MergeTriplet> printSuperstep(DataSet<MergeTriplet> iteration) {
    DataSet<MergeTriplet> superstepPrinter = iteration
        .first(1)
        .filter(new RichFilterFunction<MergeTriplet>() {
          private Integer superstep = null;
          @Override
          public void open(Configuration parameters) throws Exception {
            this.superstep = getIterationRuntimeContext().getSuperstepNumber();
          }
          @Override
          public boolean filter(MergeTriplet vertex) throws Exception {
            LOG.info("Superstep: " + superstep);
            return false;
          }
        });

    return iteration.union(superstepPrinter);//.map(x->x);
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

    EdgeSimilarityFunction simFunction = new EdgeSimilarityFunction(
        Constants.DEFAULT_VALUE,
        Constants.MAXIMAL_GEO_DISTANCE); // todo agg mode?

    SimilarityComputation<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> similarityComputation = new SimilarityComputation
        .SimilarityComputationBuilder<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.EDGE_SIM)
        .build();

    // vertices with min sim, some triplets get omitted -> error cause
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newBaseTriplets = oldHashCcTriplets
        .runOperation(similarityComputation)
//        SimilarityComputation
//        .computeSimilarities(oldHashCcTriplets, Constants.DEFAULT_VALUE)
        .map(new AggSimValueTripletMapFunction(Constants.IGNORE_MISSING_PROPERTIES, // old mean function
            Constants.MIN_LABEL_PRIORITY_SIM))
        .withForwardedFields("f0;f1;f2;f3");

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
        .with(new RightMinusLeftSideJoinFunction<>())
        .union(newRepresentativeVertices);
  }
}
