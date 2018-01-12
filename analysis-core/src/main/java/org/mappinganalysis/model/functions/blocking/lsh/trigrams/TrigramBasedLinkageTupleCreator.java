package org.mappinganalysis.model.functions.blocking.lsh.trigrams;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.lsh.structure.LinkageTuple;

/**
 * Based on trigrams per vertex create a trigram to long value dictionary.
 * Dictionary used to create LinkageTuple objects containing BloomFilter
 * with the BitSets (reflecting the trigrams).
 */
public class TrigramBasedLinkageTupleCreator implements CustomUnaryOperation<Vertex<Long, ObjectMap>, LinkageTuple> {
  private boolean isIdfOptimizeEnabled;
  private DataSet<Vertex<Long, ObjectMap>> vertices;

  public TrigramBasedLinkageTupleCreator(boolean isIdfOptimizeEnabled) {
    this.isIdfOptimizeEnabled = isIdfOptimizeEnabled;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    this.vertices = inputData;
  }

  @Override
  public DataSet<LinkageTuple> createResult() {

    return vertices
        .runOperation(new TrigramsPerVertexCreatorWithIdfOptimization(isIdfOptimizeEnabled))
        .runOperation(new LongValueTrigramDictionaryCreator())
        .groupBy(0)
        .reduceGroup(new TrigramPositionsToBitSetReducer());
  }

}
