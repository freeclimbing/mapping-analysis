package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;

import java.util.HashMap;

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

  public IdfBlockingOperation(Integer support) {
    this.support = support;
  }

  @Override
  public void setInput(DataSet<MergeMusicTuple> inputData) {
    this.inputData = inputData;
  }

  @Override
  public DataSet<MergeMusicTriplet> createResult() {
    DataSet<Tuple2<String, Double>> idfValues = inputData
        .runOperation(new TfIdfComputer(STOP_WORDS));

    DataSet<Tuple2<Long, ObjectMap>> idfExtracted = inputData
        .map(new HighIDFValueMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf");

    DataSet<Edge<Long, Integer>> idfSupportEdges = idfExtracted
        .flatMap(new VertexIdfSingleValueExtractor())
        .groupBy(1)
        .reduceGroup(new IdfBasedEdgeCreator())
        .groupBy(0, 1)
        .sum(2)
        .filter(new SupportFilterFunction(support));

    // edge values are not longer important,
    // filter was the last step where the support was needed
    return inputData.join(idfSupportEdges)
        .where(0).equalTo(0)
        .with(new JoinIdfFirstFunction())
        .join(inputData)
        .where(1).equalTo(0)
        .with(new JoinIdfSecondFunction());
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
      HashMap<String, Double> idfs = tuple.f1.getIDFs();
      for (String idf : idfs.keySet()) {
        out.collect(new Tuple2<>(tuple.f0, idf));
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
