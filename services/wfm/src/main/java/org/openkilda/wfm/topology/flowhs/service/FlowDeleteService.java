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

package org.openkilda.wfm.topology.flowhs.service;

import org.openkilda.floodlight.flow.response.FlowErrorResponse;
import org.openkilda.floodlight.flow.response.FlowResponse;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.wfm.CommandContext;
import org.openkilda.wfm.share.flow.resources.FlowResourcesManager;
import org.openkilda.wfm.share.utils.FsmExecutor;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteContext;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteFsm;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteFsm.Event;
import org.openkilda.wfm.topology.flowhs.fsm.delete.FlowDeleteFsm.State;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class FlowDeleteService {
    @VisibleForTesting
    final Map<String, FlowDeleteFsm> fsms = new HashMap<>();
    private final FsmExecutor<FlowDeleteFsm, State, Event, FlowDeleteContext> controllerExecutor
            = new FsmExecutor<>(Event.NEXT);

    private final FlowDeleteHubCarrier carrier;
    private final PersistenceManager persistenceManager;
    private final FlowResourcesManager flowResourcesManager;

    public FlowDeleteService(FlowDeleteHubCarrier carrier, PersistenceManager persistenceManager,
                             FlowResourcesManager flowResourcesManager) {
        this.carrier = carrier;
        this.persistenceManager = persistenceManager;
        this.flowResourcesManager = flowResourcesManager;
    }

    /**
     * Handles request for flow delete.
     *
     * @param key    command identifier.
     * @param flowId the flow to delete.
     */
    public void handleRequest(String key, CommandContext commandContext, String flowId) {
        log.debug("Handling flow delete request with key {}", key);

        if (fsms.containsKey(key)) {
            log.error("Attempt to create fsm with key {}, while there's another active fsm with the same key.", key);
            return;
        }

        FlowDeleteFsm fsm = FlowDeleteFsm.newInstance(commandContext, carrier, persistenceManager,
                flowResourcesManager);
        fsms.put(key, fsm);

        controllerExecutor.fire(fsm, Event.NEXT, FlowDeleteContext.builder()
                .flowId(flowId)
                .build());

        removeIfFinished(fsm, key);
    }

    /**
     * Handles async response from worker.
     *
     * @param key command identifier.
     */
    public void handleAsyncResponse(String key, FlowResponse flowResponse) {
        log.debug("Received command completion message {}", flowResponse);
        FlowDeleteFsm fsm = fsms.get(key);
        if (fsm == null) {
            log.warn("Failed to find fsm: received response with key {} for non pending fsm", key);
            return;
        }

        if (flowResponse instanceof FlowErrorResponse) {
            controllerExecutor.fire(fsm, Event.ERROR_RECEIVED, FlowDeleteContext.builder()
                    .flowResponse(flowResponse)
                    .build());
        } else {
            controllerExecutor.fire(fsm, Event.RESPONSE_RECEIVED, FlowDeleteContext.builder()
                    .flowResponse(flowResponse)
                    .build());
        }

        removeIfFinished(fsm, key);
    }

    /**
     * Handles timeout case.
     *
     * @param key command identifier.
     */
    public void handleTimeout(String key) {
        log.debug("Handling timeout for {}", key);
        FlowDeleteFsm fsm = fsms.get(key);
        if (fsm == null) {
            log.warn("Failed to find fsm: timeout event for non pending fsm with key {}", key);
            return;
        }

        controllerExecutor.fire(fsm, Event.TIMEOUT, null);

        removeIfFinished(fsm, key);
    }

    private void removeIfFinished(FlowDeleteFsm fsm, String key) {
        if (fsm.getCurrentState() == State.FINISHED
                || fsm.getCurrentState() == State.FINISHED_WITH_ERROR) {
            log.debug("FSM with key {} is finished with state {}", key, fsm.getCurrentState());
            fsms.remove(key);

            carrier.cancelTimeoutCallback(key);
        }
    }
}
