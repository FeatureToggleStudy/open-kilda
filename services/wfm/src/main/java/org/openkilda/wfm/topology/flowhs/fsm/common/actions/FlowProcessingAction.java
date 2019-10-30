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

package org.openkilda.wfm.topology.flowhs.fsm.common.actions;

import static java.lang.String.format;

import org.openkilda.model.Flow;
import org.openkilda.model.FlowPath;
import org.openkilda.model.PathId;
import org.openkilda.persistence.FetchStrategy;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.persistence.repositories.FlowPathRepository;
import org.openkilda.persistence.repositories.FlowRepository;
import org.openkilda.persistence.repositories.RepositoryFactory;
import org.openkilda.wfm.share.history.model.FlowDumpData;
import org.openkilda.wfm.share.history.model.FlowEventData;
import org.openkilda.wfm.share.history.model.FlowHistoryData;
import org.openkilda.wfm.share.history.model.FlowHistoryHolder;
import org.openkilda.wfm.topology.flowhs.exception.FlowProcessingException;
import org.openkilda.wfm.topology.flowhs.fsm.common.FlowProcessingStateMachine;

import lombok.extern.slf4j.Slf4j;
import org.squirrelframework.foundation.fsm.AnonymousAction;

import java.time.Instant;

@Slf4j
public abstract class FlowProcessingAction<T extends FlowProcessingStateMachine<T, S, E, C>, S, E, C>
        extends AnonymousAction<T, S, E, C> {
    protected final PersistenceManager persistenceManager;
    protected final FlowRepository flowRepository;
    protected final FlowPathRepository flowPathRepository;

    public FlowProcessingAction(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
        RepositoryFactory repositoryFactory = persistenceManager.getRepositoryFactory();
        this.flowRepository = repositoryFactory.createFlowRepository();
        this.flowPathRepository = repositoryFactory.createFlowPathRepository();
    }

    @Override
    public final void execute(S from, S to, E event, C context, T stateMachine) {
        try {
            perform(from, to, event, context, stateMachine);
        } catch (Exception ex) {
            String errorMessage = format("%s: %s", getGenericErrorMessage(), ex.getMessage());
            log.error(errorMessage, ex);
            saveHistory(stateMachine, errorMessage);
            stateMachine.fireError(errorMessage);
        }
    }

    protected abstract void perform(S from, S to, E event, C context, T stateMachine);

    protected Flow getFlow(String flowId) {
        return flowRepository.findById(flowId)
                .orElseThrow(() -> new FlowProcessingException(format("Flow %s not found", flowId)));
    }

    protected Flow getFlow(String flowId, FetchStrategy fetchStrategy) {
        return flowRepository.findById(flowId, fetchStrategy)
                .orElseThrow(() -> new FlowProcessingException(format("Flow %s not found", flowId)));
    }

    protected FlowPath getFlowPath(Flow flow, PathId pathId) {
        return flow.getPath(pathId)
                .orElseThrow(() -> new FlowProcessingException(format("Flow path %s not found", pathId)));
    }

    protected FlowPath getFlowPath(PathId pathId) {
        return flowPathRepository.findById(pathId)
                .orElseThrow(() -> new FlowProcessingException(format("Flow path %s not found", pathId)));
    }

    /**
     * Returns a message for generic error that may happen during action execution.
     * The message is being returned as the execution result.
     */
    protected String getGenericErrorMessage() {
        return "Failed to process flow request";
    }

    protected void saveHistory(T stateMachine, String action) {
        saveHistory(stateMachine, action, null);
    }

    protected void saveHistory(T stateMachine, String action, String description) {
        FlowHistoryHolder historyHolder = FlowHistoryHolder.builder()
                .taskId(stateMachine.getCommandContext().getCorrelationId())
                .flowHistoryData(FlowHistoryData.builder()
                        .action(action)
                        .time(Instant.now())
                        .flowId(stateMachine.getFlowId())
                        .description(description)
                        .build())
                .build();
        stateMachine.getCarrier().sendHistoryUpdate(historyHolder);
    }

    protected void saveHistoryWithEvent(T stateMachine, String action, FlowEventData.Event event) {
        Instant timestamp = Instant.now();
        FlowHistoryHolder historyHolder = FlowHistoryHolder.builder()
                .taskId(stateMachine.getCommandContext().getCorrelationId())
                .flowHistoryData(FlowHistoryData.builder()
                        .action(action)
                        .time(timestamp)
                        .flowId(stateMachine.getFlowId())
                        .build())
                .flowEventData(FlowEventData.builder()
                        .flowId(stateMachine.getFlowId())
                        .event(event)
                        .time(timestamp)
                        .build())
                .build();
        stateMachine.getCarrier().sendHistoryUpdate(historyHolder);
    }

    protected void saveHistoryWithDump(T stateMachine, String action, String description, FlowDumpData flowDumpData) {
        FlowHistoryHolder historyHolder = FlowHistoryHolder.builder()
                .taskId(stateMachine.getCommandContext().getCorrelationId())
                .flowDumpData(flowDumpData)
                .flowHistoryData(FlowHistoryData.builder()
                        .action(action)
                        .time(Instant.now())
                        .description(description)
                        .flowId(stateMachine.getFlowId())
                        .build())
                .build();
        stateMachine.getCarrier().sendHistoryUpdate(historyHolder);
    }
}
