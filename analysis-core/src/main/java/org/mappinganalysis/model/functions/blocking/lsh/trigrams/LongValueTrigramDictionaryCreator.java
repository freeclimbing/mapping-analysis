package org.mappinganalysis.model.functions.blocking.lsh.trigrams;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.mappinganalysis.model.functions.CharSet;

/**
 * Creates a long-trigram dictionary. For each vertex, the trigram set (CharSet) is
 * flatmapped to return tuple2(vertexId, trigramId) for each contained element.
 */
public class LongValueTrigramDictionaryCreator
    implements CustomUnaryOperation<Tuple2<Long, CharSet>, Tuple2<Long, Long>> {
  private DataSet<Tuple2<Long, CharSet>> trigramsPerVertex;

  @Override
  public void setInput(DataSet<Tuple2<Long, CharSet>> inputData) {
    this.trigramsPerVertex = inputData;
  }

  @Override
  public DataSet<Tuple2<Long, Long>> createResult() {
    DataSet<Tuple1<char[]>> singleTrigrams = trigramsPerVertex
        .flatMap((FlatMapFunction<Tuple2<Long, CharSet>, Tuple1<char[]>>)
            (value, out) -> {
              for (char[] chars : value.f1) {
                out.collect(new Tuple1<>(chars));
              }
            })
        .returns(new TypeHint<Tuple1<char[]>>() {});

    DataSet<Tuple2<Long, char[]>> idTrigramsDict = DataSetUtils
        .zipWithUniqueId(singleTrigrams.distinct())
        .map(dictEntry -> new Tuple2<>(dictEntry.f0, dictEntry.f1.f0))
        .returns(new TypeHint<Tuple2<Long, char[]>>() {});

    return trigramsPerVertex
        .flatMap((FlatMapFunction<Tuple2<Long, CharSet>, Tuple2<Long, char[]>>)
            (vertex, out) -> {
              for (char[] chars : vertex.f1) {
                out.collect(new Tuple2<>(vertex.f0, chars));
              }
            })
        .returns(new TypeHint<Tuple2<Long, char[]>>() {})
        .join(idTrigramsDict)
        .where(1)
        .equalTo(1)
        .with((vertexIdTrigram, idTrigramDictEntry) ->
            new Tuple2<>(vertexIdTrigram.f0, idTrigramDictEntry.f0))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});
  }
}
