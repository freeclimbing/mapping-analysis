package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;

/**
 * tf idf blocking
 */
public class IdfBlockingOperation
    implements CustomUnaryOperation<MergeTuple, MergeTriplet> {
  private static final Logger LOG = Logger.getLogger(IdfBlockingOperation.class);
  private DataSet<MergeTuple> inputData;
  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la",
      // old: "love", "de", "no", "best", "music", "live", "hits", "from", "collection", "your", "unknown", "volume"
      "de", "no", "unknown"
  };
  private Integer support;
  private ExecutionEnvironment env;

  public IdfBlockingOperation(Integer support, ExecutionEnvironment env) {
    this.support = support;
    this.env = env;
  }

  @Override
  public void setInput(DataSet<MergeTuple> inputData) {
    this.inputData = inputData;
  }

  @Override
  public DataSet<MergeTriplet> createResult() {
    DataSet<Tuple2<Long, String>> preparedInputData = inputData
        .map(new PrepareInputMapper());

    DataSet<Tuple2<String, Double>> idfValues = preparedInputData
        .runOperation(new TfIdfComputer(STOP_WORDS));

    DataSet<String> returns = idfValues.map(tuple -> tuple.f0)
        .returns(new TypeHint<String>() {});
    DataSet<Tuple2<Long, String>> tuple2DataSet = DataSetUtils.zipWithUniqueId(returns);

    /**
     * Best 2(+) tf idf values for a artist title album line
     */
    DataSet<Tuple2<Long, Long>> idfExtracted = preparedInputData
        .flatMap(new HighIDFValueFlatMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf")
        .withBroadcastSet(tuple2DataSet, "idfDict");

    DataSet<Edge<Long, Integer>> firstPart = idfExtracted
        .groupBy(1) // idf string
        .reduceGroup(new IdfBasedEdgeCreator());

    DataSet<Edge<Long, Integer>> idfSupportEdges = firstPart
        .groupBy(0, 1)
        .sum(2)
        .filter(new SupportFilterFunction(support));

    // create cc graph
    DataSet<Vertex<Long, Long>> ccVertices = createCcBlockingKeyVertices(idfSupportEdges);

//    DataSet<Tuple2<Long, Long>> sum = ccVertices
//        .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
//      @Override
//      public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
//        return new Tuple2<>(vertex.getValue(), 1L);
//      }
//    })
//        .groupBy(0)
//        .sum(1);

    // edge values are not longer important,
    // filter was the last step where the support was needed
    return inputData.join(idfSupportEdges)
        .where(0).equalTo(0)
        .with(new JoinIdfFirstFunction())
        .join(inputData)
        .where(1).equalTo(0)
        .with(new JoinIdfSecondFunction())
        .join(ccVertices)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<MergeTriplet, Vertex<Long, Long>, MergeTriplet>() {
          @Override
          public MergeTriplet join(MergeTriplet triplet, Vertex<Long, Long> second) throws Exception {
            triplet.setBlockingLabel(second.getValue().toString());
            return triplet;
          }
        });
  }

  private DataSet<Vertex<Long, Long>> createCcBlockingKeyVertices(DataSet<Edge<Long, Integer>> idfSupportEdges) {
    DataSet<Edge<Long, NullValue>> edges = idfSupportEdges
        .map(new MapFunction<Edge<Long, Integer>, Edge<Long, NullValue>>() {
      @Override
      public Edge<Long, NullValue> map(Edge<Long, Integer> value) throws Exception {
        return new Edge<>(value.getSource(), value.getTarget(), NullValue.getInstance());
      }
    });

    DataSet<Vertex<Long, Long>> vertices = edges
        .flatMap(new FlatMapFunction<Edge<Long, NullValue>, Vertex<Long, Long>>() {
      @Override
      public void flatMap(Edge<Long, NullValue> value, Collector<Vertex<Long, Long>> out) throws Exception {
        out.collect(new Vertex<>(value.getSource(), value.getSource()));
        out.collect(new Vertex<>(value.getTarget(), value.getTarget()));
      }
    }).distinct();

    Graph<Long, Long, NullValue> ccGraph = Graph.fromDataSet(vertices, edges, env);

    DataSet<Vertex<Long, Long>> ccVertices = null;
    try {
      ccVertices = ccGraph.run(new GSAConnectedComponents<>(Integer.MAX_VALUE));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ccVertices;
  }

  /**
   * From the set of idf values within a vertex,
   * extract single values to tuple for further grouping
   */
  @Deprecated
  private static class VertexIdfSingleValueExtractor
      implements FlatMapFunction<Tuple2<Long, ObjectMap>, Tuple2<Long, String>> {
    @Override
    public void flatMap(Tuple2<Long, ObjectMap> tuple,
                        Collector<Tuple2<Long, String>> out) throws Exception {
//      HashMap<String, Double> idfs = tuple.f1.getIDFs();
      for (String idfString : tuple.f1.keySet()) {
        out.collect(new Tuple2<>(tuple.f0, idfString));
      }
    }
  }

  @ForwardedFieldsSecond("f0; f1; f2->f3")
  private static class JoinIdfFirstFunction
      implements JoinFunction<MergeTuple, Edge<Long, Integer>,
      Tuple4<Long, Long, MergeTuple, Integer>> {
    @Override
    public Tuple4<Long, Long, MergeTuple, Integer> join(
        MergeTuple left,
        Edge<Long, Integer> right) throws Exception {
      return new Tuple4<>(right.f0, right.f1, left, right.f2);
    }
  }

  private static class JoinIdfSecondFunction
      implements JoinFunction<Tuple4<Long,Long,MergeTuple,Integer>,
      MergeTuple,
      MergeTriplet> {
    @Override
    public MergeTriplet join(
        Tuple4<Long, Long, MergeTuple, Integer> first,
        MergeTuple second) throws Exception {
      return new MergeTriplet(first.f2, second);
    }
  }

}
