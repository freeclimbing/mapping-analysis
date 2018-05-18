package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.ops.SinglePropertySimilarity;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;

/**
 * Music edge similarity function, {@value Constants#ARTIST_TITLE_ALBUM} used
 * for better results.
 */
public class MusicSimilarityFunction
    extends SimilarityFunction<Triplet<Long, ObjectMap, NullValue>,
    Triplet<Long, ObjectMap, ObjectMap>>
    implements Serializable {

  public MusicSimilarityFunction(String metric) {
    this.metric = metric;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet)
      throws Exception {

    EdgeObjectMapTriplet result = new EdgeObjectMapTriplet(triplet, Constants.MUSIC);

    result
//        .runOperation(new SinglePropertySimilarity(Constants.LANGUAGE, metric))
        .runOperation(new SinglePropertySimilarity(Constants.ARTIST_TITLE_ALBUM, metric))
//        .runOperation(new SinglePropertySimilarity(Constants.YEAR, metric))
//        .runOperation(new SinglePropertySimilarity(Constants.LENGTH, metric))
    ;

//    System.out.println("muSiFu: " + result.getEdge().getValue().toString());
    return result;
  }
}
