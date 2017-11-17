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
 * Music edge similarity function
 */
public class MusicSimilarityFunction
    extends SimilarityFunction<Triplet<Long, ObjectMap, NullValue>, Triplet<Long, ObjectMap, ObjectMap>>
    implements Serializable {

  public MusicSimilarityFunction() {
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet)
      throws Exception {

    EdgeObjectMapTriplet result = new EdgeObjectMapTriplet(triplet);
    result.runOperation(new SinglePropertySimilarity(Constants.LANGUAGE))
        .runOperation(new SinglePropertySimilarity(Constants.LABEL))
        .runOperation(new SinglePropertySimilarity(Constants.ARTIST))
        .runOperation(new SinglePropertySimilarity(Constants.ALBUM))
        .runOperation(new SinglePropertySimilarity(Constants.YEAR))
        .runOperation(new SinglePropertySimilarity(Constants.LENGTH));

//    System.out.println("muSiFu: " + result.getEdge().getValue().toString());
    return result;
  }
}
