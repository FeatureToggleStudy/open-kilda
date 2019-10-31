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

package org.openkilda.wfm.topology.flowhs.fsm.update;

import org.openkilda.floodlight.flow.request.InstallIngressRule;
import org.openkilda.floodlight.flow.request.InstallTransitRule;
import org.openkilda.floodlight.flow.request.RemoveRule;
import org.openkilda.floodlight.flow.response.FlowErrorResponse;
import org.openkilda.floodlight.flow.response.FlowResponse;
import org.openkilda.messaging.Message;
import org.openkilda.messaging.error.ErrorData;
import org.openkilda.messaging.error.ErrorMessage;
import org.openkilda.messaging.error.ErrorType;
import org.openkilda.model.FlowPathStatus;
import org.openkilda.model.FlowStatus;
import org.openkilda.model.PathId;
import org.openkilda.pce.PathComputer;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.wfm.CommandContext;
import org.openkilda.wfm.share.flow.resources.FlowResources;
import org.openkilda.wfm.share.flow.resources.FlowResourcesManager;
import org.openkilda.wfm.share.logger.FlowOperationsDashboardLogger;
import org.openkilda.wfm.topology.flowhs.fsm.common.NbTrackableStateMachine;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm.Event;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm.State;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.AllocatePrimaryResourcesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.AllocateProtectedResourcesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.CancelPendingCommandsAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.CompleteFlowPathInstallationAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.CompleteFlowPathRemovalAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.DeallocateResourcesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.DumpIngressRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.DumpNonIngressRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.HandleNotDeallocatedResourcesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.HandleNotRemovedPathsAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.HandleNotRemovedRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.HandleNotRevertedResourceAllocationAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.InstallIngressRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.InstallNonIngressRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.OnFinishedWithErrorAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.OnReceivedInstallResponseAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.OnReceivedRemoveResponseAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.PostResourceAllocationAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.RemoveOldRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.RevertFlowAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.RevertFlowStatusAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.RevertNewRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.RevertPathsSwapAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.RevertResourceAllocationAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.SwapFlowPathsAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.UpdateFlowAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.UpdateFlowStatusAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.ValidateFlowAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.ValidateIngressRulesAction;
import org.openkilda.wfm.topology.flowhs.fsm.update.actions.ValidateNonIngressRulesAction;
import org.openkilda.wfm.topology.flowhs.model.RequestedFlow;
import org.openkilda.wfm.topology.flowhs.service.FlowUpdateHubCarrier;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.squirrelframework.foundation.fsm.StateMachineBuilder;
import org.squirrelframework.foundation.fsm.StateMachineBuilderFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Getter
@Setter
@Slf4j
public final class FlowUpdateFsm extends NbTrackableStateMachine<FlowUpdateFsm, State, Event, FlowUpdateContext> {

    private final FlowUpdateHubCarrier carrier;
    private final String flowId;

    private RequestedFlow targetFlow;

    private FlowStatus originalFlowStatus;
    private String originalFlowGroup;
    private RequestedFlow originalFlow;

    private FlowResources newPrimaryResources;
    private FlowResources newProtectedResources;
    private PathId newPrimaryForwardPath;
    private PathId newPrimaryReversePath;
    private PathId newProtectedForwardPath;
    private PathId newProtectedReversePath;

    private Collection<FlowResources> oldResources;
    private PathId oldPrimaryForwardPath;
    private FlowPathStatus oldPrimaryForwardPathStatus;
    private PathId oldPrimaryReversePath;
    private FlowPathStatus oldPrimaryReversePathStatus;
    private PathId oldProtectedForwardPath;
    private FlowPathStatus oldProtectedForwardPathStatus;
    private PathId oldProtectedReversePath;
    private FlowPathStatus oldProtectedReversePathStatus;

    private Set<UUID> pendingCommands = Collections.emptySet();
    private Map<UUID, Integer> retriedCommands = new HashMap<>();
    private Map<UUID, FlowErrorResponse> errorResponses = new HashMap<>();

    private Map<UUID, FlowResponse> failedValidationResponses = new HashMap<>();

    private Map<UUID, InstallIngressRule> ingressCommands = new HashMap<>();
    private Map<UUID, InstallTransitRule> nonIngressCommands = new HashMap<>();
    private Map<UUID, RemoveRule> removeCommands = new HashMap<>();

    private String errorReason;

    public FlowUpdateFsm(CommandContext commandContext, FlowUpdateHubCarrier carrier, String flowId) {
        super(commandContext);
        this.carrier = carrier;
        this.flowId = flowId;
    }

    private static StateMachineBuilder<FlowUpdateFsm, State, Event, FlowUpdateContext> builder(
            PersistenceManager persistenceManager, PathComputer pathComputer, FlowResourcesManager resourcesManager,
            int transactionRetriesLimit) {
        StateMachineBuilder<FlowUpdateFsm, State, Event, FlowUpdateContext> builder =
                StateMachineBuilderFactory.create(FlowUpdateFsm.class, State.class, Event.class,
                        FlowUpdateContext.class, CommandContext.class, FlowUpdateHubCarrier.class, String.class);

        FlowOperationsDashboardLogger dashboardLogger = new FlowOperationsDashboardLogger(log);

        builder.transition().from(State.INITIALIZED).to(State.FLOW_VALIDATED).on(Event.NEXT)
                .perform(new ValidateFlowAction(persistenceManager, dashboardLogger));
        builder.transition().from(State.INITIALIZED).to(State.FINISHED_WITH_ERROR).on(Event.TIMEOUT);

        builder.transition().from(State.FLOW_VALIDATED).to(State.FLOW_UPDATED).on(Event.NEXT)
                .perform(new UpdateFlowAction(persistenceManager, transactionRetriesLimit));
        builder.transitions().from(State.FLOW_VALIDATED)
                .toAmong(State.REVERTING_FLOW_STATUS, State.REVERTING_FLOW_STATUS)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.transition().from(State.FLOW_UPDATED).to(State.PRIMARY_RESOURCES_ALLOCATED).on(Event.NEXT)
                .perform(new AllocatePrimaryResourcesAction(persistenceManager, transactionRetriesLimit,
                        pathComputer, resourcesManager, dashboardLogger));
        builder.transitions().from(State.FLOW_UPDATED)
                .toAmong(State.REVERTING_FLOW, State.REVERTING_FLOW)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.transition().from(State.PRIMARY_RESOURCES_ALLOCATED).to(State.PROTECTED_RESOURCES_ALLOCATED)
                .on(Event.NEXT)
                .perform(new AllocateProtectedResourcesAction(persistenceManager, transactionRetriesLimit,
                        pathComputer, resourcesManager, dashboardLogger));
        builder.transitions().from(State.PRIMARY_RESOURCES_ALLOCATED)
                .toAmong(State.REVERTING_ALLOCATED_RESOURCES, State.REVERTING_ALLOCATED_RESOURCES,
                        State.REVERTING_ALLOCATED_RESOURCES)
                .onEach(Event.TIMEOUT, Event.ERROR, Event.NO_PATH_FOUND);

        builder.transition().from(State.PROTECTED_RESOURCES_ALLOCATED).to(State.RESOURCE_ALLOCATION_COMPLETED)
                .on(Event.NEXT)
                .perform(new PostResourceAllocationAction(persistenceManager));
        builder.transitions().from(State.PROTECTED_RESOURCES_ALLOCATED)
                .toAmong(State.REVERTING_ALLOCATED_RESOURCES, State.REVERTING_ALLOCATED_RESOURCES,
                        State.REVERTING_ALLOCATED_RESOURCES)
                .onEach(Event.TIMEOUT, Event.ERROR, Event.NO_PATH_FOUND);

        builder.transition().from(State.RESOURCE_ALLOCATION_COMPLETED).to(State.INSTALLING_NON_INGRESS_RULES)
                .on(Event.NEXT)
                .perform(new InstallNonIngressRulesAction(persistenceManager, resourcesManager));
        builder.transitions().from(State.RESOURCE_ALLOCATION_COMPLETED)
                .toAmong(State.REVERTING_ALLOCATED_RESOURCES, State.REVERTING_ALLOCATED_RESOURCES)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.internalTransition().within(State.INSTALLING_NON_INGRESS_RULES).on(Event.RESPONSE_RECEIVED)
                .perform(new OnReceivedInstallResponseAction());
        builder.internalTransition().within(State.INSTALLING_NON_INGRESS_RULES).on(Event.ERROR_RECEIVED)
                .perform(new OnReceivedInstallResponseAction());
        builder.transition().from(State.INSTALLING_NON_INGRESS_RULES).to(State.NON_INGRESS_RULES_INSTALLED)
                .on(Event.RULES_INSTALLED);
        builder.transitions().from(State.INSTALLING_NON_INGRESS_RULES)
                .toAmong(State.PATHS_SWAP_REVERTED, State.PATHS_SWAP_REVERTED)
                .onEach(Event.TIMEOUT, Event.ERROR)
                .perform(new CancelPendingCommandsAction());

        builder.transition().from(State.NON_INGRESS_RULES_INSTALLED).to(State.VALIDATING_NON_INGRESS_RULES)
                .on(Event.NEXT)
                .perform(new DumpNonIngressRulesAction());
        builder.transitions().from(State.NON_INGRESS_RULES_INSTALLED)
                .toAmong(State.PATHS_SWAP_REVERTED, State.PATHS_SWAP_REVERTED)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.internalTransition().within(State.VALIDATING_NON_INGRESS_RULES).on(Event.RESPONSE_RECEIVED)
                .perform(new ValidateNonIngressRulesAction());
        builder.internalTransition().within(State.VALIDATING_NON_INGRESS_RULES).on(Event.ERROR_RECEIVED)
                .perform(new ValidateNonIngressRulesAction());
        builder.transition().from(State.VALIDATING_NON_INGRESS_RULES).to(State.NON_INGRESS_RULES_VALIDATED)
                .on(Event.RULES_VALIDATED);
        builder.transitions().from(State.VALIDATING_NON_INGRESS_RULES)
                .toAmong(State.PATHS_SWAP_REVERTED, State.PATHS_SWAP_REVERTED, State.PATHS_SWAP_REVERTED)
                .onEach(Event.TIMEOUT, Event.MISSING_RULE_FOUND, Event.ERROR)
                .perform(new CancelPendingCommandsAction());

        builder.transition().from(State.NON_INGRESS_RULES_VALIDATED).to(State.PATHS_SWAPPED).on(Event.NEXT)
                .perform(new SwapFlowPathsAction(persistenceManager, resourcesManager));
        builder.transitions().from(State.NON_INGRESS_RULES_VALIDATED)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.transition().from(State.PATHS_SWAPPED).to(State.INSTALLING_INGRESS_RULES).on(Event.NEXT)
                .perform(new InstallIngressRulesAction(persistenceManager, resourcesManager));
        builder.transitions().from(State.PATHS_SWAPPED)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.internalTransition().within(State.INSTALLING_INGRESS_RULES).on(Event.RESPONSE_RECEIVED)
                .perform(new OnReceivedInstallResponseAction());
        builder.internalTransition().within(State.INSTALLING_INGRESS_RULES).on(Event.ERROR_RECEIVED)
                .perform(new OnReceivedInstallResponseAction());
        builder.transition().from(State.INSTALLING_INGRESS_RULES).to(State.INGRESS_RULES_INSTALLED)
                .on(Event.RULES_INSTALLED);
        builder.transition().from(State.INSTALLING_INGRESS_RULES).to(State.INGRESS_RULES_VALIDATED)
                .on(Event.INGRESS_IS_SKIPPED);
        builder.transitions().from(State.INSTALLING_INGRESS_RULES)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.ERROR)
                .perform(new CancelPendingCommandsAction());

        builder.transition().from(State.INGRESS_RULES_INSTALLED).to(State.VALIDATING_INGRESS_RULES).on(Event.NEXT)
                .perform(new DumpIngressRulesAction());
        builder.transitions().from(State.INGRESS_RULES_INSTALLED)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.internalTransition().within(State.VALIDATING_INGRESS_RULES).on(Event.RESPONSE_RECEIVED)
                .perform(new ValidateIngressRulesAction(persistenceManager));
        builder.internalTransition().within(State.VALIDATING_INGRESS_RULES).on(Event.ERROR_RECEIVED)
                .perform(new ValidateIngressRulesAction(persistenceManager));
        builder.transition().from(State.VALIDATING_INGRESS_RULES).to(State.INGRESS_RULES_VALIDATED)
                .on(Event.RULES_VALIDATED);
        builder.transitions().from(State.VALIDATING_INGRESS_RULES)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.MISSING_RULE_FOUND, Event.ERROR)
                .perform(new CancelPendingCommandsAction());

        builder.transition().from(State.INGRESS_RULES_VALIDATED).to(State.NEW_PATHS_INSTALLATION_COMPLETED)
                .on(Event.NEXT)
                .perform(new CompleteFlowPathInstallationAction(persistenceManager));
        builder.transitions().from(State.INGRESS_RULES_VALIDATED)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.transition().from(State.NEW_PATHS_INSTALLATION_COMPLETED).to(State.REMOVING_OLD_RULES).on(Event.NEXT)
                .perform(new RemoveOldRulesAction(persistenceManager, resourcesManager));
        builder.transitions().from(State.NEW_PATHS_INSTALLATION_COMPLETED)
                .toAmong(State.REVERTING_PATHS_SWAP, State.REVERTING_PATHS_SWAP)
                .onEach(Event.TIMEOUT, Event.ERROR);

        builder.internalTransition().within(State.REMOVING_OLD_RULES).on(Event.RESPONSE_RECEIVED)
                .perform(new OnReceivedRemoveResponseAction());
        builder.internalTransition().within(State.REMOVING_OLD_RULES).on(Event.ERROR_RECEIVED)
                .perform(new OnReceivedRemoveResponseAction());
        builder.transition().from(State.REMOVING_OLD_RULES).to(State.OLD_RULES_REMOVED)
                .on(Event.RULES_REMOVED);
        builder.transition().from(State.REMOVING_OLD_RULES).to(State.OLD_RULES_REMOVED)
                .on(Event.ERROR)
                .perform(new HandleNotRemovedRulesAction());

        builder.transition().from(State.OLD_RULES_REMOVED).to(State.OLD_PATHS_REMOVAL_COMPLETED).on(Event.NEXT)
                .perform(new CompleteFlowPathRemovalAction(persistenceManager, transactionRetriesLimit));

        builder.transition().from(State.OLD_PATHS_REMOVAL_COMPLETED).to(State.DEALLOCATING_OLD_RESOURCES)
                .on(Event.NEXT);
        builder.transition().from(State.OLD_PATHS_REMOVAL_COMPLETED).to(State.DEALLOCATING_OLD_RESOURCES)
                .on(Event.ERROR)
                .perform(new HandleNotRemovedPathsAction());

        builder.transition().from(State.DEALLOCATING_OLD_RESOURCES).to(State.OLD_RESOURCES_DEALLOCATED).on(Event.NEXT)
                .perform(new DeallocateResourcesAction(persistenceManager, resourcesManager));

        builder.transition().from(State.OLD_RESOURCES_DEALLOCATED).to(State.UPDATING_FLOW_STATUS).on(Event.NEXT);
        builder.transition().from(State.OLD_RESOURCES_DEALLOCATED).to(State.UPDATING_FLOW_STATUS)
                .on(Event.ERROR)
                .perform(new HandleNotDeallocatedResourcesAction());

        builder.transition().from(State.UPDATING_FLOW_STATUS).to(State.FLOW_STATUS_UPDATED).on(Event.NEXT)
                .perform(new UpdateFlowStatusAction(persistenceManager, dashboardLogger));

        builder.transition().from(State.FLOW_STATUS_UPDATED).to(State.FINISHED).on(Event.NEXT);
        builder.transition().from(State.FLOW_STATUS_UPDATED).to(State.FINISHED_WITH_ERROR).on(Event.ERROR);

        builder.transition().from(State.REVERTING_PATHS_SWAP).to(State.PATHS_SWAP_REVERTED)
                .on(Event.NEXT)
                .perform(new RevertPathsSwapAction(persistenceManager));

        builder.transitions().from(State.PATHS_SWAP_REVERTED)
                .toAmong(State.REVERTING_NEW_RULES, State.REVERTING_NEW_RULES)
                .onEach(Event.NEXT, Event.ERROR)
                .perform(new RevertNewRulesAction(persistenceManager, resourcesManager));

        builder.internalTransition().within(State.REVERTING_NEW_RULES).on(Event.RESPONSE_RECEIVED)
                .perform(new OnReceivedRemoveResponseAction());
        builder.internalTransition().within(State.REVERTING_NEW_RULES).on(Event.ERROR_RECEIVED)
                .perform(new OnReceivedRemoveResponseAction());
        builder.transition().from(State.REVERTING_NEW_RULES).to(State.NEW_RULES_REVERTED)
                .on(Event.RULES_REMOVED);
        builder.transition().from(State.REVERTING_NEW_RULES).to(State.NEW_RULES_REVERTED)
                .on(Event.ERROR)
                .perform(new HandleNotRemovedRulesAction());

        builder.transitions().from(State.NEW_RULES_REVERTED)
                .toAmong(State.REVERTING_ALLOCATED_RESOURCES, State.REVERTING_ALLOCATED_RESOURCES)
                .onEach(Event.NEXT, Event.ERROR);

        builder.transitions().from(State.REVERTING_ALLOCATED_RESOURCES)
                .toAmong(State.RESOURCES_ALLOCATION_REVERTED, State.RESOURCES_ALLOCATION_REVERTED)
                .onEach(Event.NEXT, Event.ERROR)
                .perform(new RevertResourceAllocationAction(persistenceManager, resourcesManager));
        builder.transition().from(State.RESOURCES_ALLOCATION_REVERTED).to(State.REVERTING_FLOW).on(Event.NEXT);
        builder.transition().from(State.RESOURCES_ALLOCATION_REVERTED).to(State.REVERTING_FLOW)
                .on(Event.ERROR)
                .perform(new HandleNotRevertedResourceAllocationAction());

        builder.transitions().from(State.REVERTING_FLOW)
                .toAmong(State.REVERTING_FLOW_STATUS, State.REVERTING_FLOW_STATUS)
                .onEach(Event.NEXT, Event.ERROR)
                .perform(new RevertFlowAction(persistenceManager));

        builder.transitions().from(State.REVERTING_FLOW_STATUS)
                .toAmong(State.FINISHED_WITH_ERROR, State.FINISHED_WITH_ERROR)
                .onEach(Event.NEXT, Event.ERROR)
                .perform(new RevertFlowStatusAction(persistenceManager));

        builder.defineFinalState(State.FINISHED);
        builder.defineFinalState(State.FINISHED_WITH_ERROR)
                .addEntryAction(new OnFinishedWithErrorAction(dashboardLogger));

        return builder;
    }

    @Override
    protected void afterTransitionCausedException(State fromState, State toState, Event event,
                                                  FlowUpdateContext context) {
        String errorMessage = getLastException().getMessage();
        if (fromState == State.INITIALIZED || fromState == State.FLOW_VALIDATED) {
            ErrorData error = new ErrorData(ErrorType.INTERNAL_ERROR, "Could not update flow", errorMessage);
            Message message = new ErrorMessage(error, getCommandContext().getCreateTime(),
                    getCommandContext().getCorrelationId());
            carrier.sendNorthboundResponse(message);
        }

        fireError(errorMessage);

        super.afterTransitionCausedException(fromState, toState, event, context);
    }

    @Override
    public void fireNext(FlowUpdateContext context) {
        fire(Event.NEXT, context);
    }

    @Override
    public void fireError(String errorReason) {
        fireError(Event.ERROR, errorReason);
    }

    private void fireError(Event errorEvent, String errorReason) {
        if (this.errorReason != null) {
            log.error("Subsequent error fired: " + errorReason);
        } else {
            this.errorReason = errorReason;
        }

        fire(errorEvent);
    }

    @Override
    public void fireNoPathFound(String errorReason) {
        fireError(Event.NO_PATH_FOUND, errorReason);
    }

    @Override
    public void sendResponse(Message message) {
        carrier.sendNorthboundResponse(message);
    }

    public static FlowUpdateFsm newInstance(CommandContext commandContext, FlowUpdateHubCarrier carrier,
                                            PersistenceManager persistenceManager,
                                            PathComputer pathComputer, FlowResourcesManager resourcesManager,
                                            int transactionRetriesLimit, String flowId) {
        return newInstance(State.INITIALIZED, commandContext, carrier,
                persistenceManager, pathComputer, resourcesManager,
                transactionRetriesLimit, flowId);
    }

    public static FlowUpdateFsm newInstance(State state, CommandContext commandContext,
                                            FlowUpdateHubCarrier carrier,
                                            PersistenceManager persistenceManager,
                                            PathComputer pathComputer, FlowResourcesManager resourcesManager,
                                            int transactionRetriesLimit, String flowId) {
        return builder(persistenceManager, pathComputer, resourcesManager, transactionRetriesLimit)
                .newStateMachine(state, commandContext, carrier, flowId);
    }

    public void addOldResources(FlowResources resources) {
        if (oldResources == null) {
            oldResources = new ArrayList<>();
        }
        oldResources.add(resources);
    }

    public enum State {
        INITIALIZED,
        FLOW_VALIDATED,
        FLOW_UPDATED,
        PRIMARY_RESOURCES_ALLOCATED,
        PROTECTED_RESOURCES_ALLOCATED,
        RESOURCE_ALLOCATION_COMPLETED,

        INSTALLING_NON_INGRESS_RULES,
        NON_INGRESS_RULES_INSTALLED,
        VALIDATING_NON_INGRESS_RULES,
        NON_INGRESS_RULES_VALIDATED,

        PATHS_SWAPPED,

        INSTALLING_INGRESS_RULES,
        INGRESS_RULES_INSTALLED,
        VALIDATING_INGRESS_RULES,
        INGRESS_RULES_VALIDATED,

        NEW_PATHS_INSTALLATION_COMPLETED,

        REMOVING_OLD_RULES,
        OLD_RULES_REMOVED,

        OLD_PATHS_REMOVAL_COMPLETED,

        DEALLOCATING_OLD_RESOURCES,
        OLD_RESOURCES_DEALLOCATED,

        UPDATING_FLOW_STATUS,
        FLOW_STATUS_UPDATED,

        FINISHED,

        REVERTING_PATHS_SWAP,
        PATHS_SWAP_REVERTED,
        REVERTING_NEW_RULES,
        NEW_RULES_REVERTED,

        REVERTING_ALLOCATED_RESOURCES,
        RESOURCES_ALLOCATION_REVERTED,
        REVERTING_FLOW_STATUS,
        REVERTING_FLOW,

        FINISHED_WITH_ERROR
    }

    public enum Event {
        NEXT,

        NO_PATH_FOUND,

        RESPONSE_RECEIVED,
        ERROR_RECEIVED,

        INGRESS_IS_SKIPPED,

        RULES_INSTALLED,
        RULES_VALIDATED,
        MISSING_RULE_FOUND,

        RULES_REMOVED,

        TIMEOUT,
        ERROR
    }
}
