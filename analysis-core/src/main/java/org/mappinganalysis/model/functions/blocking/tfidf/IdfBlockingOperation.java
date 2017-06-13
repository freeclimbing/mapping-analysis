package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;

/**
 * tf idf blocking
 */
public class IdfBlockingOperation
    implements CustomUnaryOperation<MergeMusicTuple, MergeMusicTriplet> {
  private DataSet<MergeMusicTuple> inputData;
  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la", "all"
  };
  private Integer support;
  private ExecutionEnvironment env;

  public IdfBlockingOperation(Integer support, ExecutionEnvironment env) {
    this.support = support;
    this.env = env;
  }

  @Override
  public void setInput(DataSet<MergeMusicTuple> inputData) {
    this.inputData = inputData;
  }

  @Override
  public DataSet<MergeMusicTriplet> createResult() {
    DataSet<Tuple2<String, Double>> idfValues = inputData
        .runOperation(new TfIdfComputer(STOP_WORDS));

//    DataSet<Tuple2<Long, ObjectMap>> idfExtracted = ;

    DataSet<Edge<Long, Integer>> idfSupportEdges = inputData
        .flatMap(new HighIDFValueFlatMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf")
        .groupBy(1) // idf string
        .reduceGroup(new IdfBasedEdgeCreator())
        .groupBy(0, 1)
        .sum(2)
        .filter(new SupportFilterFunction(support));

    DataSet<Edge<Long, NullValue>> edges = idfSupportEdges
        .map(new MapFunction<Edge<Long, Integer>, Edge<Long, NullValue>>() {
      @Override
      public Edge<Long, NullValue> map(Edge<Long, Integer> value) throws Exception {
        return new Edge<Long, NullValue>(value.getSource(), value.getTarget(), NullValue.getInstance());
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

    DataSet<Vertex<Long, Long>> run = null;
    try {
      run = ccGraph.run(new GSAConnectedComponents<>(Integer.MAX_VALUE));
    } catch (Exception e) {
      e.printStackTrace();
    }

    // edge values are not longer important,
        // filter was the last step where the support was needed
    assert run != null;
    return inputData.join(idfSupportEdges)
        .where(0).equalTo(0)
        .with(new JoinIdfFirstFunction())
        .join(inputData)
        .where(1).equalTo(0)
        .with(new JoinIdfSecondFunction())
        .join(run)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<MergeMusicTriplet, Vertex<Long, Long>, MergeMusicTriplet>() {
          @Override
          public MergeMusicTriplet join(MergeMusicTriplet first, Vertex<Long, Long> second) throws Exception {
            first.setBlockingLabel(second.getValue().toString());
            return first;
          }
        });
  }

  /**
   * From the set of idf values within a vertex,
   * extract single values to tuple for further grouping
   */
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
      implements JoinFunction<MergeMusicTuple, Edge<Long, Integer>,
      Tuple4<Long, Long, MergeMusicTuple, Integer>> {
    @Override
    public Tuple4<Long, Long, MergeMusicTuple, Integer> join(
        MergeMusicTuple left,
        Edge<Long, Integer> right) throws Exception {
      return new Tuple4<>(right.f0, right.f1, left, right.f2);
    }
  }

  private static class JoinIdfSecondFunction
      implements JoinFunction<Tuple4<Long,Long,MergeMusicTuple,Integer>,
      MergeMusicTuple,
      MergeMusicTriplet> {
    @Override
    public MergeMusicTriplet join(
        Tuple4<Long, Long, MergeMusicTuple, Integer> first,
        MergeMusicTuple second) throws Exception {
      return new MergeMusicTriplet(first.f2, second);
    }
  }

  private class SupportFilterFunction implements FilterFunction<Edge<Long, Integer>> {
    private Integer support;

    public SupportFilterFunction(Integer support) {
      this.support = support;
    }

    @Override
    public boolean filter(Edge<Long, Integer> value) throws Exception {
      return value.f2 > support - 1;
    }
  }
}
