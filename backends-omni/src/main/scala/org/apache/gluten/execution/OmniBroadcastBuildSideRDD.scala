package org.apache.gluten.execution

import org.apache.gluten.iterator.Iterators
import org.apache.spark.{SparkContext, broadcast}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class OmniBroadcastBuildSideRDD(
    @transient private val sc: SparkContext, 
    broadcasted: broadcast.Broadcast[BuildSideRelation])
  extends BroadcastBuildSideRDD(sc, broadcasted) {
    
  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    val relation = broadcasted.value.asReadOnlyCopy()
    Iterators
      .wrap(relation.deserialized)
      .recyclePayload(batch => batch.close())
      .create()
  }
}