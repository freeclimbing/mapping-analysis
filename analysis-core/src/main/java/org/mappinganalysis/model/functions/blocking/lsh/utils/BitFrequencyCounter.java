package org.mappinganalysis.model.functions.blocking.lsh.utils;

import com.google.common.math.DoubleMath;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.blocking.lsh.structure.LinkageTuple;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BitFrequencyCounter {
  private static final Logger LOG = Logger.getLogger(BitFrequencyCounter.class);

  private int bloomFilterLength;
	private int pruningPortionFrequentBits;
	
	public BitFrequencyCounter(int bloomFilterLength, Integer pruningPercentage){
		this.bloomFilterLength = bloomFilterLength;
		this.pruningPortionFrequentBits = pruningPercentage;
	}
	
	public DataSet<Integer> getNonFrequentBitPositions(DataSet<LinkageTuple> linkageTuples) throws Exception{
		DataSet<Tuple2<Integer, Integer>> bitFrequencies = linkageTuples
			.flatMap(new BitSplitter())
			.groupBy(0)
			.sum(1);

		int pruningPortionBits = DoubleMath.roundToInt(
        (double) pruningPortionFrequentBits / 200 * this.bloomFilterLength,
        RoundingMode.HALF_UP);

		DataSet<Tuple2<Integer, Integer>> infrequentBitFrequencies = bitFrequencies
			.sortPartition(1, Order.ASCENDING).setParallelism(1)
			.reduceGroup(new BitPruner(
					bloomFilterLength,
					pruningPortionBits));

		return infrequentBitFrequencies.map(new PositionExtractor());
	}

//	@Unused
//	private int remainingProportion(){
//		final Fraction remainingProportion = Fraction.ONE.subtract(this.pruningPortionFrequentBits);
//		final Fraction halfRemainingProportion = remainingProportion.divide(Fraction.TWO);
//		return halfRemainingProportion.multiply(this.bloomFilterLength).intValue();
//	}

	// Only frequent 1-bits are considered; frequent 0-bits
	public static class BitSplitter
      implements FlatMapFunction<LinkageTuple, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = -4938978697648927611L;

		@Override
        public void flatMap(LinkageTuple line, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            final BitSet bs = line.getBloomFilter().getBitset();
           
            /*
            final int bfSize = line.getBloomFilter().getSize();               
            for (int i = 0; i < bfSize; i++){
            	final int count = bs.get(i) ? 1 : 0;
            	out.collect(new Tuple2<Integer, Integer>(i, count));
            }
            */
            
            for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
                out.collect(new Tuple2<Integer, Integer>(i, 1));
            
                if (i == Integer.MAX_VALUE) {
                    break; // or (i+1) would overflow
                }
            }
        }
    }
	
	public static class BitPruner
      implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = -7590248223942801089L;

		private int startIndex;
		private int endIndex; 
		
		public BitPruner(int bloomFilterLength, int pruningPortionFrequentBits){
			this.startIndex = pruningPortionFrequentBits;
			this.endIndex = bloomFilterLength - this.startIndex;
		}
		
		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> values,
                       Collector<Tuple2<Integer, Integer>> out)
				throws Exception {
			int counter = 0;
			for (Tuple2<Integer, Integer> value : values) {
				if (counter > this.endIndex) {
					break;
				}
				else if (counter > this.startIndex && counter < this.endIndex) {
					out.collect(value);
				}
				counter++;
			}		
		}
		
	}

  public static class PositionExtractor
      implements MapFunction<Tuple2<Integer, Integer>, Integer> {

    private static final long serialVersionUID = 6755771548465793351L;

    @Override
    public Integer map(Tuple2<Integer, Integer> value) throws Exception {
//      LOG.info(value.toString());
      return value.f0;
    }
  }
	
	public static class PositionToListMapper
      implements MapFunction<Tuple2<Integer, Integer>, List<Integer>> {

		private static final long serialVersionUID = 6755771548465793351L;

		@Override
		public List<Integer> map(Tuple2<Integer, Integer> value) throws Exception {
			List<Integer> result = new ArrayList<Integer>();
			result.add(value.f0);
			return result;
		}   
    }
	
	public static class ListPositionReducer
      implements ReduceFunction<List<Integer>> {
		
		private static final long serialVersionUID = 8135060582955390420L;

		@Override
		public List<Integer> reduce(List<Integer> value1, List<Integer> value2) throws Exception {
			final int totalSize = value1.size() + value2.size();
			final List<Integer> result = new ArrayList<Integer>(totalSize);
			result.addAll(value1);
			result.addAll(value2);
			return result;
		}    
    }
}
