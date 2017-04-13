package org.mappinganalysis.model.functions.simcomputation.ops;

import com.google.common.collect.Lists;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.api.SimilarityOperation;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Multiple properties are used to compute a single similarity.
 */
public class MultiPropertySimilarity implements SimilarityOperation<EdgeObjectMapTriplet> {
  private EdgeObjectMapTriplet triplet;

  @Override
  public void setInput(EdgeObjectMapTriplet inputData) {
    this.triplet = inputData;
  }

  @Override
  public EdgeObjectMapTriplet createResult() {
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();
    ArrayList<String> listOne = Lists.newArrayList();
    ArrayList<String> listTwo = Lists.newArrayList();

    ObjectMap source = triplet.getSrcVertex().getValue();
    ObjectMap target = triplet.getTrgVertex().getValue();


//    Collections.addAll(listOne, )

    return null;
  }
}
