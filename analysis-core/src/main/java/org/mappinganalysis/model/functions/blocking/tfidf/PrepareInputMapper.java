package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.model.MergeTuple;

/**
 * Created by markus on 6/14/17.
 */
@FunctionAnnotation.ForwardedFieldsSecond("f0; f12->f1")
public class PrepareInputMapper
    implements MapFunction<MergeTuple, Tuple2<Long, String>> {
  @Override
  public Tuple2<Long, String> map(MergeTuple tuple) throws Exception {
    return new Tuple2<>(tuple.getId(), tuple.getArtistTitleAlbum());
  }
}
