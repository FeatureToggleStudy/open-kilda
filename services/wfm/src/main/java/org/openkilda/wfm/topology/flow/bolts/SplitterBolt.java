/* Copyright 2019 Telstra Open Source
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.openkilda.wfm.topology.flow.bolts;

import org.openkilda.messaging.Message;
import org.openkilda.messaging.Utils;
import org.openkilda.messaging.command.CommandData;
import org.openkilda.messaging.command.CommandMessage;
import org.openkilda.messaging.command.flow.DeallocateFlowResourcesRequest;
import org.openkilda.messaging.command.flow.FlowCreateRequest;
import org.openkilda.messaging.command.flow.FlowDeleteRequest;
import org.openkilda.messaging.command.flow.FlowPathSwapRequest;
import org.openkilda.messaging.command.flow.FlowReadRequest;
import org.openkilda.messaging.command.flow.FlowRerouteRequest;
import org.openkilda.messaging.command.flow.FlowUpdateRequest;
import org.openkilda.messaging.command.flow.FlowsDumpRequest;
import org.openkilda.messaging.command.flow.SwapFlowEndpointRequest;
import org.openkilda.messaging.command.flow.UpdateFlowPathStatusRequest;
import org.openkilda.messaging.info.InfoData;
import org.openkilda.messaging.info.InfoMessage;
import org.openkilda.messaging.info.flow.FlowInfoData;
import org.openkilda.messaging.info.flow.FlowOperation;
import org.openkilda.wfm.topology.flow.FlowTopology;
import org.openkilda.wfm.topology.flow.StreamType;
import org.openkilda.wfm.topology.utils.MessageKafkaTranslator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Northbound Request Bolt. Handles northbound requests.
 */
public class SplitterBolt extends BaseRichBolt {
    /**
     * The logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(SplitterBolt.class);

    /**
     * Output collector.
     */
    private OutputCollector outputCollector;

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple tuple) {
        Message message = null;

        try {
            message = (Message) tuple.getValueByField(MessageKafkaTranslator.FIELD_ID_PAYLOAD);

            if (message == null
                    || !(message instanceof CommandMessage || message instanceof InfoMessage)) {
                /*
                 * Due to refactoring the kafka topics, it appears more messages are coming to the splitter than
                 * originally desinged for.
                 *
                 * TODO(crimi): Fix the cause of excess messages coming to the splitter.
                 */
                // String message = String.format("Could not deserialize message: %s", request);
                // logger.error("{}", message, exception);
                //
                // ErrorMessage errorMessage = new ErrorMessage(
                //        new ErrorData(ErrorType.REQUEST_INVALID, message, exception.getMessage()),
                //        System.currentTimeMillis(), Utils.SYSTEM_CORRELATION_ID, Destination.NORTHBOUND);
                //
                // values = new Values(errorMessage, ErrorType.INTERNAL_ERROR);
                // outputCollector.emit(StreamType.ERROR.toString(), tuple, values);
                return;
            }

            logger.debug("Request tuple={}", tuple);

            /*
             * First, try to see if this is a PUSH / UNPUSH (smaller code base vs other).
             * NB: InfoMessage was used since it has the relevant attributes/properties for
             * pushing the flow.
             */
            if (message instanceof InfoMessage) {
                InfoData data = ((InfoMessage) message).getData();
                if (data instanceof FlowInfoData) {
                    FlowInfoData fid = (FlowInfoData) data;
                    String flowId = fid.getFlowId();

                    Values values = new Values(message, flowId);
                    logger.info("Flow {} message: operation={} values={}", flowId, fid.getOperation(), values);
                    if (fid.getOperation() == FlowOperation.PUSH
                            || fid.getOperation() == FlowOperation.PUSH_PROPAGATE) {
                        outputCollector.emit(StreamType.PUSH.toString(), tuple, values);
                    } else if (fid.getOperation() == FlowOperation.UNPUSH
                            || fid.getOperation() == FlowOperation.UNPUSH_PROPAGATE) {
                        outputCollector.emit(StreamType.UNPUSH.toString(), tuple, values);
                    } else {
                        logger.warn("Skip undefined FlowInfoData Operation {}: {}={}",
                                fid.getOperation(), Utils.CORRELATION_ID, message.getCorrelationId());
                    }
                } else {
                    logger.warn("Skip undefined InfoMessage: {}={}", Utils.CORRELATION_ID, message.getCorrelationId());
                }
                return;
            }

            /*
             * Second, it isn't an InfoMessage, so it must be a CommandMessage.
             */
            CommandData data = ((CommandMessage) message).getData();

            if (data instanceof FlowCreateRequest) {
                String flowId = ((FlowCreateRequest) data).getPayload().getFlowId();

                Values values = new Values(message, flowId);
                logger.info("Flow {} create message: values={}", flowId, values);

                outputCollector.emit(StreamType.CREATE.toString(), tuple, values);

            } else if (data instanceof FlowDeleteRequest) {
                String flowId = ((FlowDeleteRequest) data).getPayload().getFlowId();

                Values values = new Values(message, flowId);
                logger.info("Flow {} delete message: values={}", flowId, values);

                outputCollector.emit(StreamType.DELETE.toString(), tuple, values);

            } else if (data instanceof FlowUpdateRequest) {
                String flowId = ((FlowUpdateRequest) data).getPayload().getFlowId();

                Values values = new Values(message, flowId);
                logger.info("Flow {} update message: values={}", flowId, values);

                outputCollector.emit(StreamType.UPDATE.toString(), tuple, values);

            } else if (data instanceof FlowRerouteRequest) {
                String flowId = ((FlowRerouteRequest) data).getFlowId();

                Values values = new Values(message, flowId);
                logger.info("Flow {} reroute message: values={}", flowId, values);

                outputCollector.emit(StreamType.REROUTE.toString(), tuple, values);

            } else if (data instanceof FlowPathSwapRequest) {
                String flowId = ((FlowPathSwapRequest) data).getFlowId();

                Values values = new Values(message, flowId);
                logger.info("Flow {} path swap message: values={}", flowId, values);

                outputCollector.emit(StreamType.PATH_SWAP.toString(), tuple, values);

            } else if (data instanceof FlowReadRequest) {
                String flowId = ((FlowReadRequest) data).getFlowId();

                Values values = new Values(message, flowId);
                logger.info("Flow {} read message: values={}", flowId, values);

                outputCollector.emit(StreamType.READ.toString(), tuple, values);

            } else if (data instanceof FlowsDumpRequest) {
                Values values = new Values(message, null);
                logger.info("Flows dump message: values={}", values);

                outputCollector.emit(StreamType.DUMP.toString(), tuple, values);

            } else if (data instanceof DeallocateFlowResourcesRequest) {
                String flowId = ((DeallocateFlowResourcesRequest) data).getFlowId();

                logger.info("Deallocate resources for flow {}", flowId);

                Values values = new Values(message, flowId);
                outputCollector.emit(StreamType.DEALLOCATE_RESOURCES.toString(), tuple, values);
            } else if (data instanceof UpdateFlowPathStatusRequest) {
                String flowId = ((UpdateFlowPathStatusRequest) data).getFlowId();

                logger.info("Update status for flow {}", flowId);

                Values values = new Values(message, flowId);
                outputCollector.emit(StreamType.STATUS.toString(), tuple, values);
            } else if (data instanceof SwapFlowEndpointRequest) {
                logger.info("Swap endpoint for flow {} and {}",
                        ((SwapFlowEndpointRequest) data).getFirstFlow().getFlowId(),
                        ((SwapFlowEndpointRequest) data).getSecondFlow().getFlowId());
                Values values = new Values(message, null);
                outputCollector.emit(StreamType.SWAP_ENDPOINT.toString(), tuple, values);
            } else {
                logger.debug("Skip undefined CommandMessage: {}={}", Utils.CORRELATION_ID, message.getCorrelationId());
            }
        } catch (Exception e) {
            logger.error(String.format("Unhandled exception in %s", getClass().getName()), e);

        } finally {
            outputCollector.ack(tuple);

            logger.debug("Splitter message ack: component={}, stream={}, tuple={}, message={}",
                    tuple.getSourceComponent(), tuple.getSourceStreamId(), tuple, message);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamType.CREATE.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.READ.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.DUMP.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.UPDATE.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.DELETE.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.PUSH.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.UNPUSH.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.REROUTE.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.PATH_SWAP.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.ERROR.toString(), FlowTopology.fieldsMessageErrorType);
        outputFieldsDeclarer.declareStream(StreamType.DEALLOCATE_RESOURCES.toString(),
                FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.STATUS.toString(), FlowTopology.fieldsMessageFlowId);
        outputFieldsDeclarer.declareStream(StreamType.SWAP_ENDPOINT.toString(), FlowTopology.fieldsMessageFlowId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
}
