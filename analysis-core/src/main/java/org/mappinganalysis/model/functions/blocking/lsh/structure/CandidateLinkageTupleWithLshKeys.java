package org.mappinganalysis.model.functions.blocking.lsh.structure;

import org.apache.flink.api.java.tuple.Tuple2;

public class CandidateLinkageTupleWithLshKeys
    extends Tuple2<LinkageTupleWithLshKeys, LinkageTupleWithLshKeys> {
  public CandidateLinkageTupleWithLshKeys() {
  }

  public CandidateLinkageTupleWithLshKeys(LinkageTupleWithLshKeys value0, LinkageTupleWithLshKeys value1) {
    super(value0, value1);
  }

  public LinkageTupleWithLshKeys getCandidateOne() {
    return f0;
  }

  public void setCandidateOne(LinkageTupleWithLshKeys candidateOne) {
    this.f0 = candidateOne;
  }

  public LinkageTupleWithLshKeys getCandidateTwo() {
    return f1;
  }

  public void setCandidateTwo(LinkageTupleWithLshKeys candidateTwo) {
    this.f1 = candidateTwo;
  }

  public boolean hasLshKeyOverlap(){
    final LshKey[] keySetOne = f0.getLshKeys();
    final LshKey[] keySetTwo = f1.getLshKeys();


      for (int i = keySetOne.length-1; i >= 0; i--){
        if (keySetOne[i].equals(keySetTwo[i])){
          return true;
        }
      }
      return false;
  }

  //??
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("CandidateLinkageTuple [candidateOne=");
    builder.append(f0);
    builder.append(", candidateTwo=");
    builder.append(f1);
    builder.append("]");
    return builder.toString();
  }
}
