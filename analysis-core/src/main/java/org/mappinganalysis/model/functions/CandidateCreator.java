package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.lsh.LshCandidateCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.CandidateMergeTripletCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.SwitchMapFunction;
import org.mappinganalysis.model.functions.blocking.lsh.utils.VertexWithNewObjectMapFunction;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;

// TODO candidates based on blocking strategy
// TODO restrict candidates to needed properties!?

/**
 * Candidate Creator prepares MergeTuples for a domain (currently: geographic), distinguishes
 * elements into blocks according to blocking strategy and computes similarity according to
 * similarity strategy for all candidates which are named MergeTriplets. Finally, with
 * HungarianAlgorithm a selection of best candidates is done.
 */
public class CandidateCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(CandidateCreator.class);
  private BlockingStrategy blockingStrategy;
  private DataDomain domain;
  private String newSource;
  private int sourceCount;
  private ExecutionEnvironment env;
  private DataSet<Vertex<Long, ObjectMap>> inputVertices;

  /**
   * Constructor for incremental clustering, ids are not
   */
  public CandidateCreator(
      BlockingStrategy blockingStrategy,
      DataDomain domain,
      String newSource,
      int sourceCount,
      ExecutionEnvironment env) {
    this.blockingStrategy = blockingStrategy;
    this.domain = domain; // TODO USE domain
    this.newSource = newSource;
    this.sourceCount = sourceCount;
    this.env = env;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    inputVertices = inputData;
  }

  @Override
  public DataSet<MergeGeoTriplet> createResult() {
    // TODO check sim comp
    SimilarityComputation<MergeGeoTriplet,
        MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(new MergeGeoSimilarity()) // TODO check sim function
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.5) // TODO check
        .build();

    if (blockingStrategy.equals(BlockingStrategy.LSH_BLOCKING)) {
      boolean isIdfOptimizeEnabled = true;

      DataSet<Tuple2<Long, Long>> lshCandidates = inputVertices
          .map(new TempLogSourceCountPrintFunction(sourceCount))
          .map(x -> {
            if (x.getId() == 2479L || x.getId() == 2478L || x.getId() == 3640L) {
              LOG.info("pre LSH: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
          .runOperation(new LshCandidateCreator(isIdfOptimizeEnabled));

      DataSet<MergeGeoTuple> geoTuples = inputVertices
          .map(x -> {
            if (x.getId() == 2479L || x.getId() == 2478L || x.getId() == 3640L) {
              LOG.info("pre: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator(BlockingStrategy.NO_BLOCKING))
          .map(x -> {
            if (x.getId() == 2479L || x.getId() == 2478L || x.getId() == 3640L) {
              LOG.info("mgtc: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<MergeGeoTuple>() {});

      DataSet<MergeGeoTriplet> mergeGeoTriplets = lshCandidates
          .map(x -> {
            if (x.f1 == 2479L || x.f0 == 2479L || x.f0 == 2478L
                || x.f0 == 3640L|| x.f1 == 2478L || x.f1 == 3640L) {
              LOG.info("after LSH: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<Tuple2<Long, Long>>() {})
          .map(candidate -> new MergeGeoTriplet(candidate.f0, candidate.f1))
          .returns(new TypeHint<MergeGeoTriplet>() {})
          .join(geoTuples)
          .where(0)
          .equalTo(0)
          .with(new CandidateMergeTripletCreator(0))
          .join(geoTuples)
          .where(1)
          .equalTo(0)
          .with(new CandidateMergeTripletCreator(1))
          .map(new SwitchMapFunction(newSource))
          .map(x -> {
            if (x.f1 == 2479L || x.f0 == 2479L || x.f0 == 2478L
                || x.f0 == 3640L|| x.f1 == 2478L || x.f1 == 3640L) {
              LOG.info("pre sim comp: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<MergeGeoTriplet>() {})
          .runOperation(similarityComputation)
          .map(x -> {
            if (x.f1 == 2479L || x.f0 == 2479L || x.f0 == 2478L
                || x.f0 == 3640L|| x.f1 == 2478L || x.f1 == 3640L) {
              LOG.info("sim: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<MergeGeoTriplet>() {});

      DataSet<Edge<Long, NullValue>> edges = mergeGeoTriplets
          .map(candidate -> {
//              if (candidate.f0 == 3335L || candidate.f1 == 3335L) {
//                LOG.info("edge in CandidateCreator: " + candidate.toString());
//              }
              return new Edge<>(candidate.f0, candidate.f1, NullValue.getInstance());
          })
          .returns(new TypeHint<Edge<Long, NullValue>>() {});

      DataSet<Vertex<Long, ObjectMap>> vertices = null;
      try {
        vertices = Graph.fromDataSet(edges, env)
            .mapVertices(new VertexWithNewObjectMapFunction())
            .run(new ConnectedComponentIdAdder<>(env))
            .getVertices();
      } catch (Exception e) {
        e.printStackTrace();
      }

      assert vertices != null;
      return mergeGeoTriplets.join(vertices)
          .where(0)
          .equalTo(0)
          .with(new JoinFunction<MergeGeoTriplet, Vertex<Long,ObjectMap>, MergeGeoTriplet>() {
            @Override
            public MergeGeoTriplet join(MergeGeoTriplet triplet, Vertex<Long, ObjectMap> vertex) throws Exception {
              triplet.setBlockingLabel(vertex.getValue().getCcId().toString());
              triplet.getSrcTuple().setBlockingLabel(vertex.getValue().getCcId().toString());
              triplet.getTrgTuple().setBlockingLabel(vertex.getValue().getCcId().toString());

              return triplet;
            }
          })
          .groupBy(5)
          .reduceGroup(new HungarianAlgorithmReduceFunction());
    } else {

      return inputVertices
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator(blockingStrategy))
          .map(x -> {
            if (x.getId() == 237L) {
              LOG.info("pre: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<MergeGeoTuple>() {})
          .groupBy(7)
          .reduceGroup(new MergeGeoTripletCreator(
              sourceCount, newSource, true))
          .distinct(0, 1)
          .runOperation(similarityComputation)
          .map(x -> {
            if (x.getSrcId() == 237L || x.getTrgId() == 237L) {
              LOG.info("CandidateCreator: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<MergeGeoTriplet>() {})
          .groupBy(5)
          .reduceGroup(new HungarianAlgorithmReduceFunction());
    }
  }

  private static class TempLogSourceCountPrintFunction implements MapFunction<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>> {
    private int sourceCount;

    public TempLogSourceCountPrintFunction(int sourceCount) {

      this.sourceCount = sourceCount;
    }

    @Override
    public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> value) throws Exception {
      if (value.getId() == 0L) {
        LOG.info("sourceCount: " + sourceCount);
      }
      return value;
    }
  }
}
