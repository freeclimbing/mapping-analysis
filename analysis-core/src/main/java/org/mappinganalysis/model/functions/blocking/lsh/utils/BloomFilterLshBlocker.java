package org.mappinganalysis.model.functions.blocking.lsh.utils;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.blocking.lsh.structure.*;

import java.util.List;

/**
 * Class for building blocks from the bloom filter.
 * Therefore the LshBlocker hashes pieces of the bloom filter
 * and generates a blocking key.
 *
 * @author mfranke
 *
 */
public class BloomFilterLshBlocker
    extends RichFlatMapFunction<LinkageTuple, Tuple2<LshKey, LinkageTupleWithLshKeys>> {
  private static final Logger LOG = Logger.getLogger(BloomFilterLshBlocker.class);
  private static final long serialVersionUID = 5273583064581285374L;

  private HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup;
  private Integer[][] lshKeyPositions;

  public BloomFilterLshBlocker(Integer[][] lshKeyPositions){
    this.lshKeyPositions = lshKeyPositions;
    this.hashFamilyGroup = null;
  }

  @Override
  public void open(Configuration parameters) {
    final List<Integer> infrequentBits = getRuntimeContext().getBroadcastVariable("infrequentBits");
//        this.mapLshKeyPositionsToInfrequentBits(infrequentBits);
    this.buildHashFamilyGroup();
  }

  // currently not used
  private void mapLshKeyPositionsToInfrequentBits(List<Integer> infrequentBits) {
    if (infrequentBits != null && infrequentBits.size() > 0) {
      LOG.info("infreqBits: " + infrequentBits.size());

      if (! (infrequentBits.size() == 1 && infrequentBits.get(0).equals(Integer.MIN_VALUE))) {
        LOG.info("lshKeyPos.length: " + lshKeyPositions.length);

        for (int i = 0; i < lshKeyPositions.length; i++){
          for (int j = 0; j < lshKeyPositions[i].length; j++){
            LOG.info("lshKeyPos[i].length: " + lshKeyPositions[i].length);

            Integer integer = this.lshKeyPositions[i][j];
            LOG.info("i: " + i + " j: " + j + " int: " + integer);
            LOG.info("map lshkeyPos to infreq bits: " + infrequentBits.get(integer));

            this.lshKeyPositions[i][j] = infrequentBits.get(this.lshKeyPositions[i][j]);
          }
        }
      }
    }
  }

  private void buildHashFamilyGroup(){
    this.hashFamilyGroup = HashFamilyGroup.fromPositions(this.lshKeyPositions);
  }

  /**
   * Transformation of (Id, {@link BloomFilter}) tuples
   * into (KeyId, KeyValue, {@link LinkageTupleWithLshKeys}).
   * This transformation executes the first blocking step.
   */
  @Override
  public void flatMap(LinkageTuple value, Collector<Tuple2<LshKey, LinkageTupleWithLshKeys>> out) throws Exception {
    final Lsh<IndexHash> lsh = new Lsh<>(value.getBloomFilter(), this.hashFamilyGroup);
    final LshKey[] lshKeys = lsh.calculateKeys();

//    for (LshKey lshKey : lshKeys) {
//      LOG.info("single lsh: " + lshKey.toString());
//    }

//		LOG.info("lshKeys: " + lshKeys.length);
    for (int i = 0; i < lshKeys.length; i++){
      if (!lshKeys[i].getBitset().isEmpty()) {
        LinkageTupleWithLshKeys bfWithKeys = new LinkageTupleWithLshKeys();
        bfWithKeys.setLinkageTuple(value);

        if (i != 0) {
          final LshKey[] lshKeysSoFar = new LshKey[i];

          System.arraycopy(lshKeys, 0, lshKeysSoFar, 0, i);
          bfWithKeys.setLshKeys(lshKeysSoFar);
        }

        Tuple2<LshKey, LinkageTupleWithLshKeys> record = new Tuple2<>(lshKeys[i], bfWithKeys);
//			LOG.info("record: " + record);
        out.collect(record);
      }
    }
  }
}