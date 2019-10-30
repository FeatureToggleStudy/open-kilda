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

import org.openkilda.messaging.Message;
import org.openkilda.messaging.error.ErrorType;
import org.openkilda.model.Cookie;
import org.openkilda.model.Flow;
import org.openkilda.model.FlowPath;
import org.openkilda.model.FlowPathStatus;
import org.openkilda.model.SwitchId;
import org.openkilda.pce.PathComputer;
import org.openkilda.pce.PathPair;
import org.openkilda.pce.exception.RecoverableException;
import org.openkilda.pce.exception.UnroutableFlowException;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.persistence.RecoverablePersistenceException;
import org.openkilda.persistence.repositories.IslRepository;
import org.openkilda.persistence.repositories.RepositoryFactory;
import org.openkilda.persistence.repositories.SwitchPropertiesRepository;
import org.openkilda.persistence.repositories.SwitchRepository;
import org.openkilda.wfm.share.flow.resources.FlowResources;
import org.openkilda.wfm.share.flow.resources.FlowResourcesManager;
import org.openkilda.wfm.share.flow.resources.ResourceAllocationException;
import org.openkilda.wfm.share.history.model.FlowDumpData;
import org.openkilda.wfm.share.history.model.FlowDumpData.DumpType;
import org.openkilda.wfm.share.history.model.FlowHistoryData;
import org.openkilda.wfm.share.history.model.FlowHistoryHolder;
import org.openkilda.wfm.share.logger.FlowOperationsDashboardLogger;
import org.openkilda.wfm.share.mappers.HistoryMapper;
import org.openkilda.wfm.topology.flow.model.FlowPathPair;
import org.openkilda.wfm.topology.flowhs.fsm.common.NbTrackableStateMachine;
import org.openkilda.wfm.topology.flowhs.service.FlowPathBuilder;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A base for action classes that allocate resources for flow paths.
 */
@Slf4j
public abstract class BaseResourceAllocationAction<T extends NbTrackableStateMachine<T, S, E, C>, S, E, C> extends
        NbTrackableAction<T, S, E, C> {
    protected final int transactionRetriesLimit;
    protected final SwitchRepository switchRepository;
    protected final IslRepository islRepository;
    protected final PathComputer pathComputer;
    protected final FlowResourcesManager resourcesManager;
    protected final FlowPathBuilder flowPathBuilder;
    protected final FlowOperationsDashboardLogger dashboardLogger;

    public BaseResourceAllocationAction(PersistenceManager persistenceManager, int transactionRetriesLimit,
                                        PathComputer pathComputer, FlowResourcesManager resourcesManager,
                                        FlowOperationsDashboardLogger dashboardLogger) {
        super(persistenceManager);
        this.transactionRetriesLimit = transactionRetriesLimit;

        RepositoryFactory repositoryFactory = persistenceManager.getRepositoryFactory();
        switchRepository = repositoryFactory.createSwitchRepository();
        islRepository = repositoryFactory.createIslRepository();
        SwitchPropertiesRepository switchPropertiesRepository = repositoryFactory.createSwitchPropertiesRepository();
        flowPathBuilder = new FlowPathBuilder(switchRepository, switchPropertiesRepository);

        this.pathComputer = pathComputer;
        this.resourcesManager = resourcesManager;
        this.dashboardLogger = dashboardLogger;
    }

    @Override
    protected final Optional<Message> performWithResponse(S from, S to, E event, C context, T stateMachine) {
        if (!isAllocationRequired(stateMachine)) {
            return Optional.empty();
        }

        try {
            allocateInTransaction(stateMachine);

            return Optional.empty();
        } catch (UnroutableFlowException | RecoverableException ex) {
            String errorMessage = format("Not enough bandwidth or no path found for flow %s: %s",
                    stateMachine.getFlowId(), ex.getMessage());
            log.info(errorMessage);
            saveHistory(stateMachine, errorMessage);
            stateMachine.fireNoPathFound(errorMessage);

            return Optional.of(buildErrorMessage(stateMachine, ErrorType.NOT_FOUND,
                    getGenericErrorMessage(), errorMessage));
        } catch (ResourceAllocationException ex) {
            String errorMessage = format("Failed to allocate resources for flow %s: %s",
                    stateMachine.getFlowId(), ex.getMessage());
            saveHistory(stateMachine, errorMessage);
            stateMachine.fireError(errorMessage);

            return Optional.of(buildErrorMessage(stateMachine, ErrorType.INTERNAL_ERROR,
                    getGenericErrorMessage(), errorMessage));
        }
    }

    /**
     * Check whether allocation is required, otherwise it's being skipped.
     */
    protected abstract boolean isAllocationRequired(T stateMachine);

    /**
     * Perform resource allocation, returns the allocated resources.
     */
    protected abstract void allocate(T stateMachine)
            throws RecoverableException, UnroutableFlowException, ResourceAllocationException;

    /**
     * Called in a case of allocation failure.
     */
    protected abstract void onFailure(T stateMachine);

    /**
     * Perform resource allocation in transactions, returns the allocated resources.
     */
    @SneakyThrows
    private void allocateInTransaction(T stateMachine) throws RecoverableException, UnroutableFlowException,
            ResourceAllocationException {
        RetryPolicy retryPolicy = new RetryPolicy()
                .retryOn(RecoverableException.class)
                .retryOn(ResourceAllocationException.class)
                .retryOn(RecoverablePersistenceException.class)
                .withMaxRetries(transactionRetriesLimit);

        try {
            persistenceManager.getTransactionManager().doInTransaction(retryPolicy, () -> allocate(stateMachine));
        } catch (Exception ex) {
            onFailure(stateMachine);

            if (ex instanceof FailsafeException) {
                throw ex.getCause();
            } else {
                throw ex;
            }
        }
    }

    protected boolean isNotSamePath(PathPair pathPair, FlowPath forwardPath, FlowPath reversePath) {
        return !flowPathBuilder.isSamePath(pathPair.getForward(), forwardPath)
                || !flowPathBuilder.isSamePath(pathPair.getReverse(), reversePath);
    }

    protected FlowPathPair createFlowPathPair(Flow flow, PathPair pathPair, FlowResources flowResources) {
        long cookie = flowResources.getUnmaskedCookie();
        FlowPath newForwardPath = flowPathBuilder.buildFlowPath(flow, flowResources.getForward(),
                pathPair.getForward(), Cookie.buildForwardCookie(cookie));
        newForwardPath.setStatus(FlowPathStatus.IN_PROGRESS);
        FlowPath newReversePath = flowPathBuilder.buildFlowPath(flow, flowResources.getReverse(),
                pathPair.getReverse(), Cookie.buildReverseCookie(cookie));
        newReversePath.setStatus(FlowPathStatus.IN_PROGRESS);
        FlowPathPair newFlowPaths = FlowPathPair.builder().forward(newForwardPath).reverse(newReversePath).build();

        log.debug("Persisting the paths {}", newFlowPaths);

        persistenceManager.getTransactionManager().doInTransaction(() -> {
            flowPathRepository.lockInvolvedSwitches(newForwardPath, newReversePath);

            flowPathRepository.createOrUpdate(newForwardPath);
            flowPathRepository.createOrUpdate(newReversePath);

            updateIslsForFlowPath(newForwardPath, newReversePath);
        });

        return newFlowPaths;
    }

    private void updateIslsForFlowPath(FlowPath... paths) {
        for (FlowPath path : paths) {
            path.getSegments().forEach(pathSegment -> {
                log.debug("Updating ISL for the path segment: {}", pathSegment);

                updateAvailableBandwidth(pathSegment.getSrcSwitch().getSwitchId(), pathSegment.getSrcPort(),
                        pathSegment.getDestSwitch().getSwitchId(), pathSegment.getDestPort());
            });
        }
    }

    private void updateAvailableBandwidth(SwitchId srcSwitch, int srcPort, SwitchId dstSwitch, int dstPort) {
        long usedBandwidth = flowPathRepository.getUsedBandwidthBetweenEndpoints(srcSwitch, srcPort,
                dstSwitch, dstPort);
        log.debug("Updating ISL {}_{}-{}_{} with used bandwidth {}", srcSwitch, srcPort, dstSwitch, dstPort,
                usedBandwidth);
        long islAvailableBandwidth =
                islRepository.updateAvailableBandwidth(srcSwitch, srcPort, dstSwitch, dstPort, usedBandwidth);
        if (islAvailableBandwidth < 0) {
            throw new RecoverablePersistenceException(format("ISL %s_%d-%s_%d was overprovisioned",
                    srcSwitch, srcPort, dstSwitch, dstPort));
        }
    }

    protected void saveHistoryWithDump(T stateMachine, Flow flow,
                                       FlowPathPair oldFlowPaths, FlowPathPair newFlowPaths) {
        Instant timestamp = Instant.now();
        FlowDumpData oldDumpData = HistoryMapper.INSTANCE.map(flow,
                oldFlowPaths.getForward(), oldFlowPaths.getReverse());
        oldDumpData.setDumpType(DumpType.STATE_BEFORE);

        FlowDumpData newDumpData = HistoryMapper.INSTANCE.map(flow,
                newFlowPaths.getForward(), newFlowPaths.getReverse());
        newDumpData.setDumpType(DumpType.STATE_AFTER);

        Stream.of(oldDumpData, newDumpData).forEach(dumpData -> {
            FlowHistoryHolder historyHolder = FlowHistoryHolder.builder()
                    .taskId(stateMachine.getCommandContext().getCorrelationId())
                    .flowDumpData(dumpData)
                    .flowHistoryData(FlowHistoryData.builder()
                            .action("New paths were created (with allocated resources)")
                            .time(timestamp)
                            .flowId(flow.getFlowId())
                            .build())
                    .build();
            stateMachine.getCarrier().sendHistoryUpdate(historyHolder);
        });
    }
}
