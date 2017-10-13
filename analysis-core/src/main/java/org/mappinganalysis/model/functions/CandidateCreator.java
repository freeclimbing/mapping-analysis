package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;

// TODO candidates based on blocking strategy
// TODO restrict candidates to needed properties!?
public class CandidateCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private DataDomain domain;
  private DataSet<Vertex<Long, ObjectMap>> inputVertices;

  public CandidateCreator(DataDomain domain) {
    this.domain = domain;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    inputVertices = inputData;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {

    // TODO check sim function
    SimilarityFunction<MergeGeoTriplet,
        MergeGeoTriplet> simFunction =
        new MergeGeoSimilarity();

    // TODO check sim comp
    SimilarityComputation<MergeGeoTriplet,
        MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.5)
        .build();

    DataSet<MergeGeoTriplet> triplets = inputVertices
        .map(new MergeGeoTupleCreator())
        .groupBy(7)
        .reduceGroup(new MergeGeoTripletCreator(4))
        .runOperation(similarityComputation);

    DataSet<Tuple2<Long, Tuple>> uniqueLeftMatrixIds = DataSetUtils
        .zipWithUniqueId(triplets
            .project(0) // TODO tuple1 -> long??
            .distinct());

    DataSet<Tuple2<Long, Tuple>> uniqueRightMatrixIds = DataSetUtils
        .zipWithUniqueId(triplets
            .project(1)
            .distinct());

    triplets.map(new MapFunction<MergeGeoTriplet, Tuple2<Long, Long>>() {
      @Override
      public Tuple2<Long, Long> map(MergeGeoTriplet value) throws Exception {
        return null;
      }
    });

    return null;
  }

}
