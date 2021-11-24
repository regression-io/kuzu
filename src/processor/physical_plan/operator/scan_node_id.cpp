#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

void ScanNodeID::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    outDataChunk = this->resultSet->dataChunks[outDataPos.dataChunkPos];
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE, true /* isSequence */);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

bool ScanNodeID::getNextTuples() {
    metrics->executionTime.start();
    if (prevOperator) {
        prevOperator->getNextTuples();
    }
    {
        unique_lock<mutex> lock{morsel->mtx};
        auto nodeIDValues = (nodeID_t*)(outValueVector->values);
        // Fill the first nodeID in the sequence.
        if (morsel->currentOffset >= morsel->maxOffset) {
            // no more tuples to scan_node_id.
            metrics->executionTime.stop();
            return false;
        } else {
            nodeIDValues[0].label = nodeLabel;
            nodeIDValues[0].offset = morsel->currentOffset;
            outDataChunk->state->initOriginalAndSelectedSize(
                min(DEFAULT_VECTOR_CAPACITY, morsel->maxOffset - morsel->currentOffset));
            morsel->currentOffset += outDataChunk->state->selectedSize;
            metrics->executionTime.stop();
            metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
            return true;
        }
    }
}

} // namespace processor
} // namespace graphflow
