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

package org.openkilda.wfm.topology.flowhs.fsm.delete.actions;

import static java.lang.String.format;

import org.openkilda.floodlight.flow.request.RemoveRule;
import org.openkilda.floodlight.flow.response.FlowErrorResponse;
import org.openkilda.floodlight.flow.response.FlowResponse;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteContext;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteFsm;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteFsm.Event;

import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@Slf4j
public class OnErrorResponseAction extends RuleProcessingAction {
    private static final int MAX_RULE_COMMAND_RETRY_COUNT = 3;

    @Override
    protected void perform(FlowDeleteFsm.State from, FlowDeleteFsm.State to,
                           Event event, FlowDeleteContext context,
                           FlowDeleteFsm stateMachine) {
        FlowResponse response = context.getFlowResponse();
        if (response.isSuccess() || !(response instanceof FlowErrorResponse)) {
            throw new IllegalArgumentException(
                    format("Invoked %s for a success response: %s", this.getClass(), response));
        }

        UUID failedCommandId = response.getCommandId();
        RemoveRule failedCommand = stateMachine.getRemoveCommands().get(failedCommandId);
        if (failedCommand == null) {
            log.warn("Received a response for unexpected command: {}", response);
            return;
        }

        FlowErrorResponse errorResponse = (FlowErrorResponse) response;
        long cookie = getCookieForCommand(stateMachine, failedCommandId);

        int retries = stateMachine.getRetriedCommands().getOrDefault(failedCommandId, 0);
        if (retries < MAX_RULE_COMMAND_RETRY_COUNT) {
            stateMachine.getRetriedCommands().put(failedCommandId, ++retries);

            String message = format(
                    "Failed to remove rule %s from switch %s: %s. Description: %s. Retrying (attempt %d)",
                    cookie, errorResponse.getSwitchId(), errorResponse.getErrorCode(),
                    errorResponse.getDescription(),
                    retries);
            log.warn(message);
            sendHistoryUpdate(stateMachine, "Failed to remove rule", message);

            stateMachine.getCarrier().sendSpeakerRequest(failedCommand);
        } else {
            stateMachine.getPendingCommands().remove(failedCommandId);

            String message = format("Failed to remove rule %s from switch %s: %s. Description: %s",
                    cookie, errorResponse.getSwitchId(), errorResponse.getErrorCode(),
                    errorResponse.getDescription());
            log.warn(message);
            sendHistoryUpdate(stateMachine, "Failed to remove rule", message);

            stateMachine.getErrorResponses().put(failedCommandId, errorResponse);

            if (stateMachine.getPendingCommands().isEmpty()) {
                log.warn("Received error response(s) for some remove commands of the flow {}",
                        stateMachine.getFlowId());
                stateMachine.fireError();
            }
        }
    }
}
