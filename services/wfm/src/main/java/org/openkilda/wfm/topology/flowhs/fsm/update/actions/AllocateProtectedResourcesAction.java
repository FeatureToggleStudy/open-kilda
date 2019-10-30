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

package org.openkilda.wfm.topology.flowhs.fsm.update.actions;

import static java.lang.String.format;

import org.openkilda.messaging.error.ErrorType;
import org.openkilda.model.Flow;
import org.openkilda.model.FlowPath;
import org.openkilda.model.PathId;
import org.openkilda.pce.PathComputer;
import org.openkilda.pce.PathPair;
import org.openkilda.pce.exception.RecoverableException;
import org.openkilda.pce.exception.UnroutableFlowException;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.wfm.share.flow.resources.FlowResources;
import org.openkilda.wfm.share.flow.resources.FlowResourcesManager;
import org.openkilda.wfm.share.flow.resources.ResourceAllocationException;
import org.openkilda.wfm.share.logger.FlowOperationsDashboardLogger;
import org.openkilda.wfm.topology.flow.model.FlowPathPair;
import org.openkilda.wfm.topology.flowhs.exception.FlowProcessingException;
import org.openkilda.wfm.topology.flowhs.fsm.common.actions.BaseResourceAllocationAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateContext;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm.Event;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm.State;
import org.openkilda.wfm.topology.flowhs.model.RequestedFlow;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class AllocateProtectedResourcesAction extends
        BaseResourceAllocationAction<FlowUpdateFsm, State, Event, FlowUpdateContext> {
    public AllocateProtectedResourcesAction(PersistenceManager persistenceManager, int transactionRetriesLimit,
                                            PathComputer pathComputer, FlowResourcesManager resourcesManager,
                                            FlowOperationsDashboardLogger dashboardLogger) {
        super(persistenceManager, transactionRetriesLimit, pathComputer, resourcesManager, dashboardLogger);
    }

    @Override
    protected boolean isAllocationRequired(FlowUpdateFsm stateMachine) {
        return stateMachine.getTargetFlow().isAllocateProtectedPath();
    }

    @Override
    protected void allocate(FlowUpdateFsm stateMachine)
            throws RecoverableException, UnroutableFlowException, ResourceAllocationException {
        String flowId = stateMachine.getFlowId();
        RequestedFlow targetFlow = stateMachine.getTargetFlow();
        Flow flow = flowRepository.findById(flowId).orElseThrow(() -> new FlowProcessingException(ErrorType.NOT_FOUND,
                "Could not create a new path", format("Flow %s not found", flowId)));

        PathId newPrimaryForwardPathId = stateMachine.getNewPrimaryForwardPath();
        FlowPath primaryForwardPath = flow.getPath(newPrimaryForwardPathId)
                .orElseThrow(() -> new FlowProcessingException(ErrorType.NOT_FOUND,
                        "Could not find a newly created path",
                        format("Flow path %s not found", newPrimaryForwardPathId)));
        PathId newPrimaryReversePathId = stateMachine.getNewPrimaryReversePath();
        FlowPath primaryReversePath = flow.getPath(newPrimaryReversePathId)
                .orElseThrow(() -> new FlowProcessingException(ErrorType.NOT_FOUND,
                        "Could not find a newly created path",
                        format("Flow path %s not found", newPrimaryReversePathId)));

        List<PathId> pathsToReuse = Stream.of(flow.getProtectedForwardPathId(), flow.getProtectedReversePathId())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        log.debug("Finding a new protected path for flow {}", flowId);
        PathPair potentialPath = pathComputer.getPath(flow, pathsToReuse);

        boolean overlappingProtectedPathFound =
                flowPathBuilder.arePathsOverlapped(potentialPath.getForward(), primaryForwardPath)
                        || flowPathBuilder.arePathsOverlapped(potentialPath.getReverse(), primaryReversePath);
        if (overlappingProtectedPathFound) {
            log.warn("Can't find non overlapping new protected path for flow {}. Skip creating it.",
                    flow.getFlowId());

            stateMachine.setNewProtectedResources(null);
            stateMachine.setNewProtectedForwardPath(null);
            stateMachine.setNewProtectedReversePath(null);
        } else {
            log.debug("Allocating resources for a new protected path of flow {}", flowId);
            FlowResources flowResources = resourcesManager.allocateFlowResources(flow);
            log.debug("Resources have been allocated: {}", flowResources);
            stateMachine.setNewProtectedResources(flowResources);

            FlowPathPair paths = createFlowPathPair(flow, potentialPath, flowResources);
            log.debug("New protected path has been created: {}", paths);
            stateMachine.setNewProtectedForwardPath(paths.getForward().getPathId());
            stateMachine.setNewProtectedReversePath(paths.getReverse().getPathId());

            FlowPathPair oldPaths = FlowPathPair.builder()
                    .forward(flow.getForwardPath())
                    .reverse(flow.getReversePath())
                    .build();
            saveHistoryWithDump(stateMachine, flow, oldPaths, paths);
        }
    }

    @Override
    protected void onFailure(FlowUpdateFsm stateMachine) {
        stateMachine.setNewProtectedResources(null);
        stateMachine.setNewProtectedForwardPath(null);
        stateMachine.setNewProtectedReversePath(null);
    }
}
