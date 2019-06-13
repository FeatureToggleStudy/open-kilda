/* Copyright 2018 Telstra Open Source
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

package org.openkilda.wfm.error;

import org.openkilda.model.SwitchId;

public class IslNotFoundException extends Exception {
    public IslNotFoundException(SwitchId srcSwitchId, Integer srcPort, SwitchId dstSwitchId, Integer dstPort) {
        super(String.format("There is no ISL between %s-%d and %s-%d.", srcSwitchId, srcPort, dstSwitchId, dstPort));
    }

    public IslNotFoundException(String message) {
        super(message);
    }
}
