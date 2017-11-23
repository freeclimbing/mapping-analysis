package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.incremental.StableMarriageReduceFunction;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;

// TODO candidates based on blocking strategy
// TODO restrict candidates to needed properties!?
public class CandidateCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(CandidateCreator.class);
  private DataDomain domain;
  private int sourceCount;
  private DataSet<Vertex<Long, ObjectMap>> inputVertices;

  /**
   * Constructor for incremental clustering, ids are not
   */
  public CandidateCreator(DataDomain domain, int sourceCount) {
    this.domain = domain; // TODO USE domain
    this.sourceCount = sourceCount;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    inputVertices = inputData;
  }

  @Override
  public DataSet<MergeGeoTriplet> createResult() {
    // TODO check sim function
    SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet> simFunction =
        new MergeGeoSimilarity();

    // TODO check sim comp
    SimilarityComputation<MergeGeoTriplet,
        MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.0)
        .build();

    return inputVertices
        .map(new AddShadingTypeMapFunction())
        .map(new MergeGeoTupleCreator())
        .groupBy(7)
        .reduceGroup(new MergeGeoTripletCreator(sourceCount, true))
        .runOperation(similarityComputation)
        .distinct(0,1)
        .groupBy(5)
        .reduceGroup(new StableMarriageReduceFunction());
  }
}
