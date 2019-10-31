package org.openkilda.functionaltests.spec.multitable

import static groovyx.gpars.GParsPool.withPool
import static org.junit.Assume.assumeTrue
import static org.openkilda.functionaltests.extension.tags.Tag.SMOKE
import static org.openkilda.functionaltests.extension.tags.Tag.SMOKE_SWITCHES
import static org.openkilda.testing.Constants.RULES_INSTALLATION_TIME
import static org.openkilda.testing.Constants.WAIT_OFFSET

import org.openkilda.functionaltests.HealthCheckSpecification
import org.openkilda.functionaltests.extension.tags.Tags
import org.openkilda.functionaltests.helpers.PathHelper
import org.openkilda.functionaltests.helpers.Wrappers
import org.openkilda.messaging.info.event.IslChangeType
import org.openkilda.messaging.info.event.PathNode
import org.openkilda.messaging.payload.flow.FlowState
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
                assumeTrue("Should be at least two active traffgens connected to switches", false)
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

        when: "Create the prepared hybrid protected flow"
        def flow = flowHelperV2.randomFlow(swPair)
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

        and: "Delete flows"
        [flow, flow2].each { flowHelper.deleteFlow(it.flowId) }
        database.resetCosts()
    }

    def "Flow rules are (re)installed according to switch property while rerouting/syncing/updating"() {
        given: "Three active not neighboring switches"
        List<PathNode> desiredPath = null
        List<Switch> involvedSwitches = null
        def switchPair = topologyHelper.allNotNeighboringSwitchPairs.collectMany { [it, it.reversed] }.find { pair ->
            desiredPath = pair.paths.find { path ->
                involvedSwitches = pathHelper.getInvolvedSwitches(path)
                //4 switches total. First and third switches should allow multi-table
                involvedSwitches.size() == 3
            }
        }
        assumeTrue("Unable to find a path with three switches", switchPair.asBoolean())
        //make required path the most preferred
        switchPair.paths.findAll { it != desiredPath }.each { pathHelper.makePathMorePreferable(desiredPath, it) }
        def initSrcSwProps = northbound.getSwitchProperties(involvedSwitches[0].dpId)
        def initTransitSwProps = northbound.getSwitchProperties(involvedSwitches[1].dpId)
        def initDstSwProps = northbound.getSwitchProperties(involvedSwitches[2].dpId)

        and: "Multi table is enabled for them"
        def multitableSrcSwIsEnabled = initSrcSwProps.multiTable
        def multitableTransitSwIsEnabled = initTransitSwProps.multiTable
        def multitableDstSwIsEnabled = initDstSwProps.multiTable
        multitableSrcSwIsEnabled ?: northbound.updateSwitchProperties(involvedSwitches[0].dpId,
                northbound.getSwitchProperties(involvedSwitches[0].dpId).tap { it.multiTable = true })
        multitableTransitSwIsEnabled ?: northbound.updateSwitchProperties(involvedSwitches[1].dpId,
                northbound.getSwitchProperties(involvedSwitches[1].dpId).tap { it.multiTable = true })
        multitableDstSwIsEnabled ?: northbound.updateSwitchProperties(involvedSwitches[2].dpId,
                northbound.getSwitchProperties(involvedSwitches[2].dpId).tap { it.multiTable = true })

        when: "Create a flow"
        def flow = flowHelperV2.randomFlow(switchPair)
        flow.allocateProtectedPath = true
        flowHelperV2.addFlow(flow)
        def flowPathInfo = northbound.getFlowPath(flow.flowId)
        assert PathHelper.convert(flowPathInfo) == desiredPath

        then: "Flow rules are created in multi table mode"
        def flowInfoFromDb = database.getFlow(flow.flowId)
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 4
                rules.find { it.cookie == flowInfoFromDb.protectedReversePath.cookie.value }.tableId == 4
            }
            verifyAll(northbound.getSwitchRules(involvedSwitches[1].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 6
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 6
            }
            verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 4
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb.protectedForwardPath.cookie.value }.tableId == 4
            }
        }

        when: "Update switch properties(multi_table: false) on the transit switch"
        northbound.updateSwitchProperties(involvedSwitches[1].dpId,
                northbound.getSwitchProperties(involvedSwitches[1].dpId).tap { it.multiTable = false })

        then: "Flow rules are still multi table mode"
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 4
                rules.find { it.cookie == flowInfoFromDb.protectedReversePath.cookie.value }.tableId == 4
            }
            verifyAll(northbound.getSwitchRules(involvedSwitches[1].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 6
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 6
            }
            verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 4
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb.protectedForwardPath.cookie.value }.tableId == 4
            }
        }

        when: "Syncronize the flow"
        northbound.synchronizeFlow(flow.flowId)

        then: "Rules on the transit switch are recreated in single table mode"
        def flowInfoFromDb2 = database.getFlow(flow.flowId)
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(involvedSwitches[1].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 0
                rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 0
            }
        }

        and: "Rules on the src and dst switches are still in multi table mode"
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 4
                rules.find { it.cookie == flowInfoFromDb2.protectedReversePath.cookie.value }.tableId == 4
            }
            verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 4
                rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb2.protectedForwardPath.cookie.value }.tableId == 4
            }
        }

        when: "Update switch properties(multi_table: false) on the src switch"
        northbound.updateSwitchProperties(involvedSwitches[0].dpId,
                northbound.getSwitchProperties(involvedSwitches[0].dpId).tap { it.multiTable = false })

        then: "Flow rules are still in multi table mode on the src and dst switches"
        verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 2
            rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 4
            rules.find { it.cookie == flowInfoFromDb2.protectedReversePath.cookie.value }.tableId == 4
        }
        verifyAll(northbound.getSwitchRules(involvedSwitches[1].dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 0
            rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 0
        }
        verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 4
            rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 2
            rules.find { it.cookie == flowInfoFromDb2.protectedForwardPath.cookie.value }.tableId == 4
        }

        when: "Reroute(intentional) the flow via APIv2"
        with(northboundV2.rerouteFlow(flow.flowId)) {
            !it.rerouted
        }

        then: "Flow rules on the src switch are not recreated in single table mode because the flow wasn't rerouted"
        verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 2
            rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 4
            rules.find { it.cookie == flowInfoFromDb2.protectedReversePath.cookie.value }.tableId == 4
        }
        verifyAll(northbound.getSwitchRules(involvedSwitches[1].dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 0
            rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 0
        }
        verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 4
            rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 2
            rules.find { it.cookie == flowInfoFromDb2.protectedForwardPath.cookie.value }.tableId == 4
        }

        when: "Update the flow"
        northbound.updateFlow(flow.flowId, northbound.getFlow(flow.flowId).tap {
            it.description = it.description + " updated"
        })

        then: "Flow rules on the src switch are recreated in single table mode"
        with(database.getFlow(flow.flowId)) { flowInfo ->
            Wrappers.wait(RULES_INSTALLATION_TIME) {
                verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.protectedReversePath.cookie.value }.tableId == 0
                }
                verifyAll(northbound.getSwitchRules(involvedSwitches[1].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
                }
                verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 4
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 2
                    rules.find { it.cookie == flowInfo.protectedForwardPath.cookie.value }.tableId == 4
                }
            }
        }

        when: "Update switch properties(multi_table: false) on the dst switch"
        northbound.updateSwitchProperties(involvedSwitches[2].dpId,
                northbound.getSwitchProperties(involvedSwitches[2].dpId).tap { it.multiTable = false })

        and: "Swap main and protected path"
        def currentProtectedPath = pathHelper.convert(flowPathInfo.protectedPath)
        northbound.swapFlowPath(flow.flowId)
        def newFlowPath = pathHelper.convert(northbound.getFlowPath(flow.flowId))
        assert newFlowPath == currentProtectedPath

        then: "Flow rules on the dst switch are not recreated in the single table mode"
        with(database.getFlow(flow.flowId)) { flowInfo ->
            Wrappers.wait(RULES_INSTALLATION_TIME) {
                verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.protectedReversePath.cookie.value }.tableId == 0
                }
                verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 4
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 2
                    rules.find { it.cookie == flowInfo.protectedForwardPath.cookie.value }.tableId == 4
                }
            }
        }

        when: "Update the flow"
        northbound.updateFlow(flow.flowId, northbound.getFlow(flow.flowId).tap {
            it.allocateProtectedPath = false
        }) // flow is rerouted to desirePath

        then: "Flow rules on the dst switches are recreated in single table mode"
        with(database.getFlow(flow.flowId)) { flowInfo ->
            Wrappers.wait(RULES_INSTALLATION_TIME) {
                verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
                }
                verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
                }
            }
        }

        when: "Update switch properties(multi_table: true) on the dst switch"
        northbound.updateSwitchProperties(involvedSwitches[2].dpId,
                northbound.getSwitchProperties(involvedSwitches[2].dpId).tap { it.multiTable = true })

        and: "Reroute(intentional) the flow via APIv1"
        with(northbound.rerouteFlow(flow.flowId)) {
            !it.rerouted
        }

        then: "Flow rules on the src switch are not recreated in multi table mode because the flow was rerouted"
        with(database.getFlow(flow.flowId)) { flowInfo ->
            verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
            }
            verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
            }
        }

        when: "Init auto reroute(Fail a flow ISL (bring switch port down))"
        def flowIsls = pathHelper.getInvolvedIsls(desiredPath)
        def islToBreak = flowIsls[0]
        antiflap.portDown(islToBreak.srcSwitch.dpId, islToBreak.srcPort)
        Wrappers.wait(rerouteDelay + WAIT_OFFSET) {
            assert northbound.getFlowStatus(flow.flowId).status == FlowState.UP
            assert PathHelper.convert(northbound.getFlowPath(flow.flowId)) != desiredPath
        }

        then: "Flow rules on the dst switch are recreated in multi table mode"
        with(database.getFlow(flow.flowId)) { flowInfo ->
            Wrappers.wait(RULES_INSTALLATION_TIME) {
                verifyAll(northbound.getSwitchRules(involvedSwitches[2].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 4
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 2
                }
                verifyAll(northbound.getSwitchRules(involvedSwitches[0].dpId).flowEntries) { rules ->
                    rules.find { it.cookie == flowInfo.forwardPath.cookie.value }.tableId == 0
                    rules.find { it.cookie == flowInfo.reversePath.cookie.value }.tableId == 0
                }
            }
        }

        cleanup: "Restore init switch properties and delete the flow"
        antiflap.portUp(islToBreak.srcSwitch.dpId, islToBreak.srcPort)
        flowHelper.deleteFlow(flow.flowId)
        northbound.updateSwitchProperties(involvedSwitches[0].dpId, initSrcSwProps)
        northbound.updateSwitchProperties(involvedSwitches[1].dpId, initTransitSwProps)
        northbound.updateSwitchProperties(involvedSwitches[2].dpId, initDstSwProps)
        Wrappers.wait(discoveryInterval + WAIT_OFFSET) {
            northbound.getAllLinks().each { assert it.state != IslChangeType.FAILED }
        }
        database.resetCosts()
    }

    def "Single switch flow rules are (re)installed according to switch property while rerouting/syncing/updating"() {
        given: "An active switch"
        def sw = topology.activeSwitches[0]
        def initSwProps = northbound.getSwitchProperties(sw.dpId)

        and: "Multi table mode is enabled on it"
        def multitableSwIsEnabled = initSwProps.multiTable
        multitableSwIsEnabled ?: northbound.updateSwitchProperties(sw.dpId, northbound.getSwitchProperties(sw.dpId).tap {
            it.multiTable = true
        })

        when: "Create a flow"
        def flow = flowHelperV2.singleSwitchFlow(sw)
        flowHelperV2.addFlow(flow)

        then: "Flow rules are created in multi table mode"
        def flowInfoFromDb = database.getFlow(flow.flowId)
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 2
            }
        }

        when: "Update switch properties(multi_table: false) on the switch"
        northbound.updateSwitchProperties(sw.dpId, northbound.getSwitchProperties(sw.dpId).tap { it.multiTable = false })

        then: "Flow rules are still multi table mode"
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb.forwardPath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb.reversePath.cookie.value }.tableId == 2
            }
        }

        when: "Syncronize the flow"
        northbound.synchronizeFlow(flow.flowId)

        then: "Rules on the switch are reinstalled in single table mode"
        def flowInfoFromDb2 = database.getFlow(flow.flowId)
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 0
                rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 0
            }
        }

        when: "Update switch properties(multi_table: true) on the switch"
        northbound.updateSwitchProperties(sw.dpId, northbound.getSwitchProperties(sw.dpId).tap { it.multiTable = true })

        then: "Flow rules are still in single table mode on the switch"
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb2.forwardPath.cookie.value }.tableId == 0
                rules.find { it.cookie == flowInfoFromDb2.reversePath.cookie.value }.tableId == 0
            }
        }

        when: "Update the flow"
        northbound.updateFlow(flow.flowId, northbound.getFlow(flow.flowId).tap {
            it.description = it.description + " updated"
        })

        then: "Flow rules on the switch are recreated in multi table mode"
        def flowInfoFromDb3 = database.getFlow(flow.flowId)
        Wrappers.wait(RULES_INSTALLATION_TIME) {
            verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
                rules.find { it.cookie == flowInfoFromDb3.forwardPath.cookie.value }.tableId == 2
                rules.find { it.cookie == flowInfoFromDb3.reversePath.cookie.value }.tableId == 2
            }
        }

        when: "Update switch properties(multi_table: false) on the switch"
        northbound.updateSwitchProperties(sw.dpId, northbound.getSwitchProperties(sw.dpId).tap { it.multiTable = false })

        and: "Reroute(intentional) the flow via APIv1"
        with(northbound.rerouteFlow(flow.flowId)) { !it.rerouted }

        then: "Flow rules on the switch are not recreated in single table mode the because flow wasn't rerouted"
        verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb3.forwardPath.cookie.value }.tableId == 2
            rules.find { it.cookie == flowInfoFromDb3.reversePath.cookie.value }.tableId == 2
        }

        when: "Reroute(intentional) the flow via APIv2"
        with(northboundV2.rerouteFlow(flow.flowId)) { !it.rerouted }

        then: "Flow rules on the switch are not recreated in single table mode because the flow wasn't rerouted"
        verifyAll(northbound.getSwitchRules(sw.dpId).flowEntries) { rules ->
            rules.find { it.cookie == flowInfoFromDb3.forwardPath.cookie.value }.tableId == 2
            rules.find { it.cookie == flowInfoFromDb3.reversePath.cookie.value }.tableId == 2
        }

        cleanup: "Restore init switch properties and delete the flow"
        flowHelper.deleteFlow(flow.flowId)
        northbound.updateSwitchProperties(sw.dpId, initSwProps)
    }
}
