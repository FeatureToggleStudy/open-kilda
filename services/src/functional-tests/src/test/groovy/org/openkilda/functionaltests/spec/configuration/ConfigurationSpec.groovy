package org.openkilda.functionaltests.spec.configuration

import static org.junit.Assume.assumeTrue
import static org.openkilda.functionaltests.extension.tags.Tag.HARDWARE
import static org.openkilda.functionaltests.extension.tags.Tag.LOW_PRIORITY
import static org.openkilda.testing.Constants.WAIT_OFFSET
import static org.openkilda.testing.Constants.WAIT_OFFSET
import static org.openkilda.testing.Constants.WAIT_OFFSET
import static org.openkilda.testing.Constants.WAIT_OFFSET

import org.openkilda.functionaltests.HealthCheckSpecification
import org.openkilda.functionaltests.extension.tags.Tags
import org.openkilda.functionaltests.helpers.Wrappers
import org.openkilda.messaging.error.MessageError
import org.openkilda.messaging.info.event.IslChangeType
import org.openkilda.messaging.info.event.SwitchChangeType
import org.openkilda.messaging.model.system.KildaConfigurationDto
import org.openkilda.model.FlowEncapsulationType

import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import spock.lang.Narrative
import spock.lang.Shared

@Narrative("""
Kilda configuration is a special lever that allows to change default flow encapsulation type while creating.
This spec assumes that 'transit_vlan' is always default type
""")
class ConfigurationSpec extends HealthCheckSpecification {
    @Shared
    FlowEncapsulationType defaultEncapsulationType = FlowEncapsulationType.TRANSIT_VLAN

    @Tags(HARDWARE)
    def "System takes into account default flow encapsulation type while creating a flow"() {
        when: "Create a flow without encapsulation type"
        def switchPair = topologyHelper.getAllNeighboringSwitchPairs().find {
            it.src.noviflow && !it.src.wb5164 && it.dst.noviflow && !it.dst.wb5164
        }
        def flow1 = flowHelperV2.randomFlow(switchPair)
        flow1.encapsulationType = null
        northboundV2.addFlow(flow1)

        then: "Flow is created with current default encapsulation type(transit_vlan)"
        northbound.getFlow(flow1.flowId).encapsulationType == defaultEncapsulationType.toString().toLowerCase()

        when: "Update default flow encapsulation type"
        def newFlowEncapsulationType = FlowEncapsulationType.VXLAN
        def updateResponse = northbound.updateKildaConfiguration(
                new KildaConfigurationDto(flowEncapsulationType: newFlowEncapsulationType))

        then: "Correct response is returned"
        updateResponse.flowEncapsulationType == newFlowEncapsulationType.toString().toLowerCase()

        and: "Kilda configuration is really updated"
        northbound.getKildaConfiguration().flowEncapsulationType == newFlowEncapsulationType.toString().toLowerCase()

        when: "Create a flow without encapsulation type"
        def flow2 = flowHelperV2.randomFlow(switchPair)
        flow2.encapsulationType = null
        northboundV2.addFlow(flow2)

        then: "Flow is created with new default encapsulation type(vxlan)"
        northbound.getFlow(flow2.flowId).encapsulationType == newFlowEncapsulationType.toString().toLowerCase()

        cleanup: "Restore default configuration and delete the flow"
        northbound.updateKildaConfiguration(
                new KildaConfigurationDto(flowEncapsulationType: defaultEncapsulationType))
        [flow1.flowId, flow2.flowId].each { flowHelper.deleteFlow(it) }
    }

    @Tags(LOW_PRIORITY)
    def "System doesn't allow to update kilda configuration with wrong flow encapsulation type"() {
        when: "Try to set wrong flow encapsulation type"
        def incorrectValue = "TEST"
        northbound.updateKildaConfiguration(new KildaConfigurationDto(flowEncapsulationType: incorrectValue))

        then: "Human readable error is returned"
        def e = thrown(HttpClientErrorException)
        e.statusCode == HttpStatus.BAD_REQUEST
        e.responseBodyAsString.to(MessageError).errorMessage ==
                "No enum constant org.openkilda.model.FlowEncapsulationType.$incorrectValue"
        def testIsCompleted = true

        cleanup: "Restore default configuration"
        if (!testIsCompleted) {
            northbound.updateKildaConfiguration(
                    new KildaConfigurationDto(flowEncapsulationType: defaultEncapsulationType))
        }
    }

    def "System takes into account default multi table value while connecting a new switch"() {
        /*antiflapCooldown should be rather big in order for test to understand that there is no 'antiflap' during
       isl discovery*/
        assumeTrue(antiflapCooldown >= discoveryInterval + WAIT_OFFSET / 2)

        setup: "Disconnect one of the switches and remove it from DB. Pretend this switch never existed"
        def initConf = northbound.getKildaConfiguration()
        def sw = topology.activeSwitches.first()
        def isls = topology.getRelatedIsls(sw)
        lockKeeper.knockoutSwitch(sw)
        Wrappers.wait(discoveryTimeout + WAIT_OFFSET) {
            assert northbound.getSwitch(sw.dpId).state == SwitchChangeType.DEACTIVATED
            assert northbound.getAllLinks().findAll { it.state == IslChangeType.FAILED }.size() == isls.size() * 2
        }
        isls.each { northbound.deleteLink(islUtils.toLinkParameters(it)) }
        northbound.deleteSwitch(sw.dpId, false)

        and: "Update the multi table field in kilda configuration"
        def newMultiTableValue = !initConf.useMultiTable
        northbound.updateKildaConfiguration(northbound.getKildaConfiguration().tap {
            it.useMultiTable = newMultiTableValue
        })

        when: "New switch connects"
        lockKeeper.reviveSwitch(sw)
        Wrappers.wait(discoveryInterval + WAIT_OFFSET) {
            assert northbound.getSwitch(sw.dpId).state == SwitchChangeType.ACTIVATED
            def allIsls = northbound.getAllLinks()
            isls.each {
                assert islUtils.getIslInfo(allIsls, it).get().state == IslChangeType.DISCOVERED
                assert islUtils.getIslInfo(allIsls, it.reversed).get().state == IslChangeType.DISCOVERED
            }
        }

        then: "Switch is added with switch property according to the kilda configuration"
        northbound.getSwitchProperties(sw.dpId).multiTable == newMultiTableValue

        when: "Delete the switch and remove it from DB"
        lockKeeper.knockoutSwitch(sw)
        Wrappers.wait(discoveryTimeout + WAIT_OFFSET) {
            assert northbound.getSwitch(sw.dpId).state == SwitchChangeType.DEACTIVATED
            assert northbound.getAllLinks().findAll { it.state == IslChangeType.FAILED }.size() == isls.size() * 2
        }
        isls.each { northbound.deleteLink(islUtils.toLinkParameters(it)) }
        northbound.deleteSwitch(sw.dpId, false)

        and: "Update the multi table field in kilda configuration"
        northbound.updateKildaConfiguration(initConf)

        and: "New switch connects"
        lockKeeper.reviveSwitch(sw)
        Wrappers.wait(discoveryInterval + WAIT_OFFSET) {
            assert northbound.getSwitch(sw.dpId).state == SwitchChangeType.ACTIVATED
            def allIsls = northbound.getAllLinks()
            isls.each {
                assert islUtils.getIslInfo(allIsls, it).get().state == IslChangeType.DISCOVERED
                assert islUtils.getIslInfo(allIsls, it.reversed).get().state == IslChangeType.DISCOVERED
            }
        }

        then: "Switch is added with switch property according to the kilda configuration"
        northbound.getSwitchProperties(sw.dpId).multiTable == initConf.useMultiTable
    }
}
