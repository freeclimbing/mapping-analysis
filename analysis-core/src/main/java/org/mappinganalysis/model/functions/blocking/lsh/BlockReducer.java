package org.mappinganalysis.model.functions.blocking.lsh;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.blocking.lsh.structure.CandidateLinkageTupleWithLshKeys;
import org.mappinganalysis.model.functions.blocking.lsh.structure.LinkageTupleWithLshKeys;
import org.mappinganalysis.model.functions.blocking.lsh.structure.LshKey;

import java.util.ArrayList;

/**
 * GroupReducer for building candidate pairs that have the same value for a specific {@link LshKey}.
 * @author mfranke
 *
 */
public class BlockReducer
    extends RichGroupReduceFunction<Tuple2<LshKey, LinkageTupleWithLshKeys>,
    Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> {
  private static final Logger LOG = Logger.getLogger(BlockReducer.class);

  private static final long serialVersionUID = -788582704739580670L;

	private LongCounter blockCounter = new LongCounter();
	private LongCounter candidateCounter = new LongCounter();

	@Override
	public void open(Configuration parameters) throws Exception {
		final RuntimeContext context = getRuntimeContext();
		context.addAccumulator("block-counter", this.blockCounter);
		context.addAccumulator("candidate-counter", this.candidateCounter);
	}
	
	public void reduce(Iterable<Tuple2<LshKey, LinkageTupleWithLshKeys>> values,
                     Collector<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> out) throws Exception {

    ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>> valueList = this.cacheValues(values);
    this.blockCounter.add(1L);
    final LshKey lshKey = valueList.get(0).f0;

    for (int i = 0; i < valueList.size(); i++){
      for (int j = i + 1; j < valueList.size(); j++){

        final Tuple2<LshKey, LinkageTupleWithLshKeys> first = valueList.get(i);
        final Tuple2<LshKey, LinkageTupleWithLshKeys> second = valueList.get(j);

        final LinkageTupleWithLshKeys firstTuple = first.f1;
        final LinkageTupleWithLshKeys secondTuple = second.f1;

//        if(!firstTuple.dataSetIdEquals(secondTuple)){
          CandidateLinkageTupleWithLshKeys candidate;

//          if (firstTuple.getDataSetId().compareTo(secondTuple.getDataSetId()) < 0){
            candidate = new CandidateLinkageTupleWithLshKeys(firstTuple, secondTuple);
//          }
//          else{
//            candidate = new CandidateLinkageTupleWithLshKeys(secondTuple, firstTuple);
//          }

          this.candidateCounter.add(1L);
          out.collect(new Tuple2<>(lshKey, candidate));
        }
      }
  }

	private ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>> cacheValues(
			Iterable<Tuple2<LshKey, LinkageTupleWithLshKeys>> values){
		
		final ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>> result =
        new ArrayList<>();
		
		for (Tuple2<LshKey, LinkageTupleWithLshKeys> value : values) {
			result.add(value);
		}
		
		return result;
	}
}