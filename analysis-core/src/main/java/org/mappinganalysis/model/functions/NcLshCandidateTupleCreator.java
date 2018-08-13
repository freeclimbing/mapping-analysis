package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.lsh.LshCandidateCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.CandidateNcMergeTripletCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.VertexWithNewObjectMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

public class NcLshCandidateTupleCreator
    implements CustomUnaryOperation<MergeTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(NcLshCandidateTupleCreator.class);
  private SimilarityComputation<MergeMusicTriplet, MergeMusicTriplet> similarityComputation;
  private ExecutionEnvironment env;
  private DataSet<MergeTuple> inputTuples;
  private int valueRangeLsh;
  private int numberOfFamilies;
  private int numberOfHashesPerFamily;

  /**
   * Constructor for incremental clustering, ids are not (???)
   */
  public NcLshCandidateTupleCreator(
      SimilarityComputation<MergeMusicTriplet, MergeMusicTriplet> similarityComputation,
      ExecutionEnvironment env) {
    this.similarityComputation = similarityComputation;
//    this.domain = domain; // TODO USE domain
    this.env = env;
  }

  public NcLshCandidateTupleCreator(
      SimilarityComputation<MergeMusicTriplet, MergeMusicTriplet> similarityComputation,
      int valueRangeLsh,
      int numberOfFamilies,
      int numberOfHashesPerFamily,
      ExecutionEnvironment env) {
    this.similarityComputation = similarityComputation;
    this.valueRangeLsh = valueRangeLsh;
    this.numberOfFamilies = numberOfFamilies;
    this.numberOfHashesPerFamily = numberOfHashesPerFamily;
    this.env = env;
  }

  @Override
  public void setInput(DataSet<MergeTuple> inputData) {
    inputTuples = inputData;
  }

  @Override
  public DataSet<MergeMusicTriplet> createResult() {
    boolean isIdfOptimizeEnabled = true;
    /* LOG */
    boolean isLogEnabled = false;

    DataSet<Tuple2<Long, Long>> lshCandidates = inputTuples
//          .map(x -> {
//            if (isLogEnabled) {// && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L)) {
//              LOG.info("pre LSH: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeMusicTuple>() {})
        .map(tuple -> {
          String lshString = tuple.getArtist().concat(Constants.WHITE_SPACE)
              .concat(tuple.getLabel()).concat(Constants.WHITE_SPACE)
              .concat(tuple.getAlbum());

          return new Tuple2<>(tuple.getId(), Utils.simplify(lshString));
        })
        .returns(new TypeHint<Tuple2<Long, String>>() {})
        .runOperation(new LshCandidateCreator(
            isIdfOptimizeEnabled,
            valueRangeLsh,
            numberOfFamilies,
            numberOfHashesPerFamily));

    DataSet<MergeMusicTriplet> mergeMusicTriplets = lshCandidates
//          .map(x -> {
//            if (isLogEnabled) { // && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
////                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
//              LOG.info("after LSH: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<Tuple2<Long, Long>>() {
//          })
        .map(candidate -> new MergeMusicTriplet(candidate.f0, candidate.f1))
        .returns(new TypeHint<MergeMusicTriplet>() {})
        .join(inputTuples)
        .where(0)
        .equalTo(0)
        .with(new CandidateNcMergeTripletCreator(0))
        .join(inputTuples)
        .where(1)
        .equalTo(0)
        .with(new CandidateNcMergeTripletCreator(1))
          .map(x -> {
            if (isLogEnabled) {// && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
//              LOG.info("pre sim comp: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<MergeMusicTriplet>() {})
        ;

//    try {
//      LOG.info("count MMT: " + mergeMusicTriplets.count());
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    MapOperator<MergeMusicTriplet, MergeMusicTriplet> finalMergeMusicT = mergeMusicTriplets
        .runOperation(similarityComputation)
        .map(x -> {
          if (isLogEnabled) { // && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
            LOG.info("sim: " + x.toString());
          }
          return x;
        })
        .returns(new TypeHint<MergeMusicTriplet>() {
        });

    DataSet<Tuple1<Long>> coveredTuples = finalMergeMusicT
        .flatMap(new FlatMapFunction<MergeMusicTriplet, Tuple1<Long>>() {
          @Override
          public void flatMap(MergeMusicTriplet triplet, Collector<Tuple1<Long>> out) throws Exception {
            for (Long src : triplet.getSrcTuple().getClusteredElements()) {
              out.collect(new Tuple1<>(src));
            }
            for (Long trg : triplet.getTrgTuple().getClusteredElements()) {
              out.collect(new Tuple1<>(trg));
            }
          }
        })
        .distinct()
          .map(x -> {
            if (isLogEnabled && x.f0 <= 206972389) { // && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L)) {
              LOG.info("covered Tuple: " + x.toString());
            }
            return x;
          })
          .returns(new TypeHint<Tuple1<Long>>() {})
        ;

    DataSet<Tuple2<Long, Long>> inputCidContainedVertexTuple = inputTuples
        .flatMap(new FlatMapFunction<MergeTuple, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(MergeTuple cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
            for (Long vertex : cluster.getClusteredElements()) {
//                LOG.info("flat geo tuple ITER: " + cluster.toString());
              out.collect(new Tuple2<>(cluster.f0, vertex));
            }
          }
        })
        .distinct();

    DataSet<MergeMusicTriplet> recovered = inputCidContainedVertexTuple
        .leftOuterJoin(coveredTuples)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple2<Long, Long>>() {
          @Override
          public void join(Tuple2<Long, Long> left,
                           Tuple1<Long> right,
                           Collector<Tuple2<Long, Long>> out) throws Exception {
            if (right == null) {
//                LOG.info("flat geo tuple: " + left.toString());
//                if (left.f1 == 298L || left.f1 == 299L || left.f1 == 5013L || left.f1 == 5447L) {
//                  LOG.info("loj: " + left.toString());
//                }
              out.collect(left); // cluster, containedVertex
            }
          }
        })
        .join(inputTuples)
        .where(0) // contained vertex
        .equalTo(0)
        .with((left, right) -> right)
        .returns(new TypeHint<MergeTuple>() {})
        .map(tuple -> {
          MergeMusicTriplet newTriplet = new MergeMusicTriplet(tuple, tuple, 0d);
          if (newTriplet.f0 == null
              || newTriplet.f1 == null
              || newTriplet.f2 == null
              || newTriplet.f3 == null
              || newTriplet.f4 == null
              || newTriplet.f5 == null) {
            LOG.info("field null for tuple: " + newTriplet.f0);
            LOG.info(newTriplet.toString());
          }
          LOG.info(newTriplet.toString());
          if (tuple.f0 <= 206972389) {
            // TODO change to string method, add mode
//            LOG.info("recovered tuple: " + tuple.toString());
          }
          return newTriplet;
        })
        .returns(new TypeHint<MergeMusicTriplet>() {});

    mergeMusicTriplets = mergeMusicTriplets.union(recovered)
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)
//                && x.f0 == x.f1.longValue()) {
//              LOG.info("recovered tuple: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeMusicTriplet>() {})
        .distinct(0,1);

    DataSet<Edge<Long, NullValue>> edges = mergeMusicTriplets
        .map(x -> {
          if (isLogEnabled) {// && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//              (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
                      if (x.f0 <= 206972389)
            LOG.info("edge: " + x.toString());
          }
          return new Edge<>(x.f0, x.f1, NullValue.getInstance());
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
    return mergeMusicTriplets.join(vertices)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<MergeMusicTriplet, Vertex<Long,ObjectMap>, MergeMusicTriplet>() {
          @Override
          public MergeMusicTriplet join(MergeMusicTriplet triplet, Vertex<Long, ObjectMap> vertex) throws Exception {
            triplet.setBlockingLabel(vertex.getValue().getCcId().toString());
            triplet.getSrcTuple().setBlockingLabel(vertex.getValue().getCcId().toString());
            triplet.getTrgTuple().setBlockingLabel(vertex.getValue().getCcId().toString());

            return triplet;
          }
        });
  }
}
