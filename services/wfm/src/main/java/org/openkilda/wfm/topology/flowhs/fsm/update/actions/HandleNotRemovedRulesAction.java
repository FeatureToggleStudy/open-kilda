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

import org.openkilda.floodlight.flow.request.InstallIngressRule;
import org.openkilda.floodlight.flow.request.RemoveRule;
import org.openkilda.floodlight.flow.response.FlowErrorResponse;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateContext;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm.Event;
import org.openkilda.wfm.topology.flowhs.fsm.update.FlowUpdateFsm.State;

import lombok.extern.slf4j.Slf4j;
import org.squirrelframework.foundation.fsm.AnonymousAction;

import java.util.HashSet;
import java.util.UUID;

@Slf4j
public class HandleNotRemovedRulesAction extends AnonymousAction<FlowUpdateFsm, State, Event, FlowUpdateContext> {
    @Override
    public void execute(State from, State to, Event event, FlowUpdateContext context, FlowUpdateFsm stateMachine) {
        if (!stateMachine.getPendingCommands().isEmpty() || !stateMachine.getErrorResponses().isEmpty()) {
            for (UUID commandId : stateMachine.getPendingCommands()) {
                RemoveRule nonDeletedRule = stateMachine.getRemoveCommands().get(commandId);
                if (nonDeletedRule != null) {
                    log.warn("Failed to remove {} from the switch {}", nonDeletedRule, nonDeletedRule.getSwitchId());
                } else {
                    InstallIngressRule ingressRule = stateMachine.getIngressCommands().get(commandId);
                    log.warn("Failed to reinstall ingress {} on the switch {}", ingressRule, ingressRule.getSwitchId());
                }
            }

            for (UUID commandId : stateMachine.getErrorResponses().keySet()) {
                FlowErrorResponse errorResponse = stateMachine.getErrorResponses().get(commandId);
                log.warn("Failed to remove {} from the switch {}", errorResponse, errorResponse.getSwitchId());
            }
        }

        log.info("Canceling all pending commands: {}", stateMachine.getPendingCommands());
        stateMachine.setPendingCommands(new HashSet<>());
    }
}
