package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.GeographicMajorityPropertiesGroupReduceFunction;
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
  private static final Logger LOG = Logger.getLogger(MergeInitialization.class);


  private DataSet<Vertex<Long, ObjectMap>> vertices;
  private DataDomain domain;

  public MergeInitialization(DataDomain domain) {
    this.domain = domain;
  }

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
    if (domain == DataDomain.MUSIC || domain == DataDomain.NC) {
      return vertices;
    } else if (domain == DataDomain.GEOGRAPHY) {
      DataSet<Triplet<Long, ObjectMap, NullValue>> oldHashCcTriplets = vertices
          .filter(new OldHashCcFilterFunction())
          .groupBy(new OldHashCcKeySelector())
          .reduceGroup(new TripletCreateGroupReduceFunction());

      return rejoinSingleVertexClustersFromSimSort(vertices, oldHashCcTriplets);
    } else {
      return null;
    }
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
        Constants.GEO,
        Constants.MAXIMAL_GEO_DISTANCE); // todo agg mode?

    SimilarityComputation<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> similarityComputation = new SimilarityComputation
        .SimilarityComputationBuilder<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.EDGE_SIM)
        .build();

    // vertices with min sim, some triplets get omitted -> error cause
    // similarity was always hard set to 0.5 in MappingAnalysisExample
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newBaseTriplets = oldHashCcTriplets
        .runOperation(similarityComputation)
        .map(new AggSimValueTripletMapFunction(true, 0.5))
        .withForwardedFields("f0;f1;f2;f3");

    // only very low similarity pairs should not be merged
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newRepresentativeTriplets = newBaseTriplets
        .filter(new MinRequirementThresholdFilterFunction(0.5));

    // reduce to single representative, some vertices are now missing
    DataSet<Vertex<Long, ObjectMap>> newRepresentativeVertices = newRepresentativeTriplets
        .flatMap(new VertexExtractFlatMapFunction())
        .groupBy(new OldHashCcKeySelector())
        .reduceGroup(new GeographicMajorityPropertiesGroupReduceFunction());

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
