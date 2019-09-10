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

package org.openkilda.wfm.topology.applications;

import org.openkilda.applications.AppMessage;
import org.openkilda.messaging.Message;
import org.openkilda.persistence.PersistenceManager;
import org.openkilda.persistence.spi.PersistenceProvider;
import org.openkilda.wfm.LaunchEnvironment;
import org.openkilda.wfm.topology.AbstractTopology;
import org.openkilda.wfm.topology.applications.bolt.AppsManager;
import org.openkilda.wfm.topology.applications.bolt.NorthboundEncoder;
import org.openkilda.wfm.topology.applications.bolt.NotificationsEncoder;
import org.openkilda.wfm.topology.applications.bolt.SpeakerEncoder;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

public class AppsTopology extends AbstractTopology<AppsTopologyConfig> {

    private final int parallelism;

    public AppsTopology(LaunchEnvironment env) {
        super(env, AppsTopologyConfig.class);

        parallelism = topologyConfig.getNewParallelism();
    }

    /**
     * App topology factory.
     */
    @Override
    public StormTopology createTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        inputSpout(topologyBuilder);
        inputNbSpout(topologyBuilder);

        PersistenceManager persistenceManager =
                PersistenceProvider.getInstance().createPersistenceManager(configurationProvider);
        appsManager(topologyBuilder, persistenceManager);

        outputNorthbound(topologyBuilder);
        outputSpeaker(topologyBuilder);
        outputNotification(topologyBuilder);

        return topologyBuilder.createTopology();
    }

    private void inputNbSpout(TopologyBuilder topologyBuilder) {
        KafkaSpout<String, Message> spout = buildKafkaSpout(
                topologyConfig.getKafkaApplicationsNbTopic(), ComponentId.APPS_NB_SPOUT.toString());
        topologyBuilder.setSpout(ComponentId.APPS_NB_SPOUT.toString(), spout, parallelism);
    }

    private void inputSpout(TopologyBuilder topologyBuilder) {
        KafkaSpout<String, AppMessage> spout = buildKafkaSpoutForAppMessage(
                topologyConfig.getKafkaApplicationsTopic(), ComponentId.APPS_SPOUT.toString());
        topologyBuilder.setSpout(ComponentId.APPS_SPOUT.toString(), spout, parallelism);
    }

    private void appsManager(TopologyBuilder topologyBuilder, PersistenceManager persistenceManager) {
        AppsManager bolt = new AppsManager(persistenceManager);
        topologyBuilder.setBolt(AppsManager.BOLT_ID, bolt, parallelism)
                .shuffleGrouping(ComponentId.APPS_SPOUT.toString())
                .shuffleGrouping(ComponentId.APPS_NB_SPOUT.toString());
    }

    private void outputNorthbound(TopologyBuilder topologyBuilder) {
        NorthboundEncoder bolt = new NorthboundEncoder();
        topologyBuilder.setBolt(NorthboundEncoder.BOLT_ID, bolt, parallelism)
                .shuffleGrouping(AppsManager.BOLT_ID, NorthboundEncoder.INPUT_STREAM_ID);

        KafkaBolt output = buildKafkaBolt(topologyConfig.getKafkaNorthboundTopic());
        topologyBuilder.setBolt(ComponentId.NORTHBOUND_OUTPUT.toString(), output, parallelism)
                .shuffleGrouping(NorthboundEncoder.BOLT_ID);
    }

    private void outputSpeaker(TopologyBuilder topologyBuilder) {
        SpeakerEncoder bolt = new SpeakerEncoder();
        topologyBuilder.setBolt(SpeakerEncoder.BOLT_ID, bolt, parallelism)
                .shuffleGrouping(AppsManager.BOLT_ID, SpeakerEncoder.INPUT_STREAM_ID);

        KafkaBolt output = buildKafkaBolt(topologyConfig.getKafkaSpeakerTopic());
        topologyBuilder.setBolt(ComponentId.SPEAKER_OUTPUT.toString(), output, parallelism)
                .shuffleGrouping(SpeakerEncoder.BOLT_ID);
    }

    private void outputNotification(TopologyBuilder topologyBuilder) {
        NotificationsEncoder bolt = new NotificationsEncoder();
        topologyBuilder.setBolt(NotificationsEncoder.BOLT_ID, bolt, parallelism)
                .shuffleGrouping(AppsManager.BOLT_ID, NotificationsEncoder.INPUT_STREAM_ID);

        KafkaBolt output = buildKafkaBoltWithAppMessageSupport(topologyConfig.getKafkaAppsNotificationTopic());
        topologyBuilder.setBolt(ComponentId.NOTIFICATION_OUTPUT.toString(), output, parallelism)
                .shuffleGrouping(NotificationsEncoder.BOLT_ID);
    }

    public enum ComponentId {
        APPS_SPOUT("apps.spout"),
        APPS_NB_SPOUT("apps.nb.spout"),

        APPS_MANAGER("apps.manager"),

        NORTHBOUND_ENCODER("nb.encoder"),
        NORTHBOUND_OUTPUT("nb.output"),

        SPEAKER_ENCODER("speaker.encoder"),
        SPEAKER_OUTPUT("speaker.output"),

        NOTIFICATION_ENCODER("notification.encoder"),
        NOTIFICATION_OUTPUT("notification.output");

        private final String value;

        ComponentId(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * App topology uploader.
     */
    public static void main(String[] args) {
        try {
            LaunchEnvironment env = new LaunchEnvironment(args);
            (new AppsTopology(env)).setup();
        } catch (Exception e) {
            System.exit(handleLaunchException(e));
        }
    }
}
