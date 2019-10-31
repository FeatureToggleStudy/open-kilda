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
import org.openkilda.messaging.error.ErrorData;
import org.openkilda.messaging.error.ErrorMessage;
import org.openkilda.messaging.error.ErrorType;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.wfm.CommandContext;
import org.openkilda.wfm.topology.flowhs.exception.FlowProcessingException;
import org.openkilda.wfm.topology.flowhs.fsm.common.NbTrackableStateMachine;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public abstract class NbTrackableAction<T extends NbTrackableStateMachine<T, S, E, C>, S, E, C>
        extends FlowProcessingAction<T, S, E, C> {

    public NbTrackableAction(PersistenceManager persistenceManager) {
        super(persistenceManager);
    }

    @Override
    protected final void perform(S from, S to, E event, C context, T stateMachine) {
        Optional<Message> message = Optional.empty();
        try {
            message = performWithResponse(from, to, event, context, stateMachine);
        } catch (FlowProcessingException ex) {
            String errorMessage = format("%s: %s", ex.getErrorMessage(), ex.getErrorDescription());
            log.info(errorMessage);
            saveHistory(stateMachine, errorMessage);
            message = Optional.of(buildErrorMessage(stateMachine, ex.getErrorType(), ex.getErrorMessage(),
                    ex.getErrorDescription()));
            stateMachine.fireError(errorMessage);
        } catch (Exception ex) {
            String errorMessage = format("%s: %s", getGenericErrorMessage(), ex.getMessage());
            log.error(errorMessage, ex);
            saveHistory(stateMachine, errorMessage);
            message = Optional.of(buildErrorMessage(stateMachine, ErrorType.INTERNAL_ERROR,
                    getGenericErrorMessage(), ex.getMessage()));
            stateMachine.fireError(errorMessage);
        } finally {
            message.ifPresent(stateMachine::sendResponse);
        }
    }

    protected abstract Optional<Message> performWithResponse(S from, S to, E event, C context, T stateMachine);

    protected Message buildErrorMessage(T stateMachine, ErrorType errorType,
                                        String errorMessage, String errorDescription) {
        CommandContext commandContext = stateMachine.getCommandContext();
        ErrorData error = new ErrorData(errorType, errorMessage, errorDescription);
        return new ErrorMessage(error, commandContext.getCreateTime(), commandContext.getCorrelationId());
    }
}
