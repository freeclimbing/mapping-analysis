package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.MajorityPropertiesGroupReduceFunction;
import org.mappinganalysis.model.functions.simcomputation.AggSimValueTripletMapFunction;
import org.mappinganalysis.model.functions.simcomputation.EdgeSimilarityFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.RightMinusLeftSideJoinFunction;
import org.mappinganalysis.util.functions.filter.OldHashCcFilterFunction;
import org.mappinganalysis.util.functions.keyselector.OldHashCcKeySelector;

/**
 * Preparation steps to initialize merge operation.
 */
public class MergeInitialization
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {

  private DataSet<Vertex<Long, ObjectMap>> vertices;

  /**
   * @param vertices input set of representative vertices
   */
  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  /**
   * Prepare vertex dataset for the following refinement step
   * @return prepared vertices
   */
  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets = vertices
        .filter(new OldHashCcFilterFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new TripletCreateGroupReduceFunction());

    return rejoinSingleVertexClustersFromSimSort(vertices, oldHashCcTriplets);
  }

  /**
   * With SimSort, we extract (potentially two) single vertices from a component.
   * Here we try to rejoin vertices which have been in one cluster previously to reduce the
   * complexity for the following merge step.
   */
  private static DataSet<Vertex<Long, ObjectMap>> rejoinSingleVertexClustersFromSimSort(
      DataSet<Vertex<Long, ObjectMap>> representativeVertices,
      DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets) {

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
    Constants.IGNORE_MISSING_PROPERTIES = true;

    // vertices with min sim, some triplets get omitted -> error cause
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newBaseTriplets = oldHashCcTriplets
        .runOperation(similarityComputation)
        .map(new AggSimValueTripletMapFunction(
            Constants.IGNORE_MISSING_PROPERTIES, // old mean function
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
