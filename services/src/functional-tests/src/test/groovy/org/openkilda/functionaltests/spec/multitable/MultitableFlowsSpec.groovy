package org.openkilda.functionaltests.spec.multitable

import static groovyx.gpars.GParsPool.withPool
import static org.junit.Assume.assumeTrue
import static org.openkilda.functionaltests.extension.tags.Tag.SMOKE
import static org.openkilda.functionaltests.extension.tags.Tag.SMOKE_SWITCHES

import org.openkilda.functionaltests.HealthCheckSpecification
import org.openkilda.functionaltests.extension.tags.Tags
import org.openkilda.functionaltests.helpers.PathHelper
import org.openkilda.messaging.info.event.PathNode
import org.openkilda.model.SwitchFeature
import org.openkilda.northbound.dto.v1.flows.PingInput
import org.openkilda.testing.model.topology.TopologyDefinition.Switch
import org.openkilda.testing.service.traffexam.TraffExamService
import org.openkilda.testing.tools.FlowTrafficExamBuilder

import org.springframework.beans.factory.annotation.Autowired

import javax.inject.Provider

class MultitableFlowsSpec extends HealthCheckSpecification {

    @Autowired
    Provider<TraffExamService> traffExamProvider

    @Tags([SMOKE, SMOKE_SWITCHES])
    def "System can use both single-table and multi-table switches in flow path at the same time, change switch table \
mode with existing flows and hold flows of different table-mode types"() {
        given: "A potential flow on a path of 4 switches: multi -> single -> multi -> single"
        List<PathNode> desiredPath = null
        List<Switch> involvedSwitches = null
        def allTraffgenSwitchIds = topology.activeTraffGens*.switchConnected.dpId ?:
                assumeTrue("Should be at least two active traffgens connected to switches",false)
        def swPair = topologyHelper.allNotNeighboringSwitchPairs.collectMany { [it, it.reversed] }.find { pair ->
            desiredPath = pair.paths.find { path ->
                involvedSwitches = pathHelper.getInvolvedSwitches(path)
                //4 switches total. First and third switches should allow multi-table
                involvedSwitches.size() == 4 && involvedSwitches[0].features.contains(SwitchFeature.MULTI_TABLE) &&
                        involvedSwitches[2].features.contains(SwitchFeature.MULTI_TABLE) &&
                        involvedSwitches[0].dpId in allTraffgenSwitchIds &&
                        involvedSwitches[-1].dpId in allTraffgenSwitchIds
            }
        }
        assumeTrue("Unable to find a path that will allow 'multi -> single -> multi -> single' switch sequence",
                swPair.asBoolean())
        //make required path the most preferred
        swPair.paths.findAll { it != desiredPath }.each { pathHelper.makePathMorePreferable(desiredPath, it) }

        //Change switch properties so that path switches are multi -> single -> multi -> single -table
        [involvedSwitches[0], involvedSwitches[2]].each {
            northbound.updateSwitchProperties(it.dpId, northbound.getSwitchProperties(it.dpId).tap {
                it.multiTable = true
            })
        }
        [involvedSwitches[1], involvedSwitches[3]].each {
            northbound.updateSwitchProperties(it.dpId, northbound.getSwitchProperties(it.dpId).tap {
                it.multiTable = false
            })
        }
        def flow = flowHelperV2.randomFlow(swPair)
        flow.allocateProtectedPath = true

        when: "Create the prepared hybrid protected flow"
        flowHelperV2.addFlow(flow)
        assert PathHelper.convert(northbound.getFlowPath(flow.flowId)) == desiredPath

        then: "Created flow is valid"
        northbound.validateFlow(flow.flowId).each { assert it.asExpected }

        and: "Involved switches pass switch validation"
        involvedSwitches.each {
            with(northbound.validateSwitch(it.dpId)) { validation ->
                validation.verifyRuleSectionsAreEmpty(["missing", "excess"])
                validation.verifyMeterSectionsAreEmpty(["missing", "excess", "misconfigured"])
            }
        }

        and: "Flow is pingable"
        verifyAll(northbound.pingFlow(flow.flowId, new PingInput())) {
            it.forward.pingSuccess
            it.reverse.pingSuccess
        }

        and: "The flow allows traffic"
        def traffExam = traffExamProvider.get()
        def examFlow1 = new FlowTrafficExamBuilder(topology, traffExam)
                .buildBidirectionalExam(flowHelperV2.toV1(flow), 1000, 5)
        withPool {
            [examFlow1.forward, examFlow1.reverse].eachParallel { direction ->
                def resources = traffExam.startExam(direction)
                direction.setResources(resources)
                assert traffExam.waitExam(direction).hasTraffic()
            }
        }

        when: "Update table mode for involved switches so that it becomes 'single -> multi -> single -> multi'"
        [involvedSwitches[0], involvedSwitches[2]].each {
            northbound.updateSwitchProperties(it.dpId, northbound.getSwitchProperties(it.dpId).tap {
                it.multiTable = false
            })
        }
        [involvedSwitches[1], involvedSwitches[3]].each {
            northbound.updateSwitchProperties(it.dpId, northbound.getSwitchProperties(it.dpId).tap {
                it.multiTable = true
            })
        }

        then: "Flow remains valid and pingable, switch validation passes"
        northbound.validateFlow(flow.flowId).each { assert it.asExpected }
        involvedSwitches.each {
            with(northbound.validateSwitch(it.dpId)) { validation ->
                validation.verifyRuleSectionsAreEmpty(["missing", "excess"])
                validation.verifyMeterSectionsAreEmpty(["missing", "excess", "misconfigured"])
            }
        }
        verifyAll(northbound.pingFlow(flow.flowId, new PingInput())) {
            it.forward.pingSuccess
            it.reverse.pingSuccess
        }

        when: "Create one more similar flow on the target path"
        def flow2 = flowHelperV2.randomFlow(swPair).tap {
            it.source.portNumber = flow.source.portNumber
            it.source.vlanId = flow.source.vlanId - 1
            it.destination.portNumber = flow.destination.portNumber
            it.destination.vlanId = flow.destination.vlanId - 1
        }
        flowHelperV2.addFlow(flow2)

        then: "Both existing flows are valid"
        [flow, flow2].each {
            northbound.validateFlow(it.flowId).each { assert it.asExpected }
        }

        and: "Involved switches pass switch validation"
        involvedSwitches.each {
            with(northbound.validateSwitch(it.dpId)) { validation ->
                validation.verifyRuleSectionsAreEmpty(["missing", "excess"])
                validation.verifyMeterSectionsAreEmpty(["missing", "excess", "misconfigured"])
            }
        }

        and: "Both flows are pingable"
        [flow, flow2].each {
            verifyAll(northbound.pingFlow(it.flowId, new PingInput())) {
                it.forward.pingSuccess
                it.reverse.pingSuccess
            }
        }

        and: "Both flows allow traffic"
        withPool {
            [examFlow1.forward, examFlow1.reverse].eachParallel { direction ->
                def resources = traffExam.startExam(direction)
                direction.setResources(resources)
                assert traffExam.waitExam(direction).hasTraffic()
            }
        }
        def examFlow2 = new FlowTrafficExamBuilder(topology, traffExam)
                .buildBidirectionalExam(flowHelperV2.toV1(flow2), 1000, 5)
        withPool {
            [examFlow2.forward, examFlow2.reverse].eachParallel { direction ->
                def resources = traffExam.startExam(direction)
                direction.setResources(resources)
                assert traffExam.waitExam(direction).hasTraffic()
            }
        }

        when: "Synchronize all involved switches"
        then:""

        and: "Delete flows"
        [flow, flow2].each { flowHelper.deleteFlow(it.flowId) }
    }
}
