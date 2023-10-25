/*
 * Copyright 2023, OpenRemote Inc.
 *
 * See the CONTRIBUTORS.txt file in the distribution for a
 * full listing of individual contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.openremote.manager.asset;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.ResourceAware;
import org.openremote.container.message.MessageBrokerService;
import org.openremote.manager.datapoint.AssetAnomalyDatapointService;
import org.openremote.manager.datapoint.AssetDatapointService;
import org.openremote.manager.event.ClientEventService;
import org.openremote.manager.gateway.GatewayService;
import org.openremote.model.Container;
import org.openremote.model.ContainerService;
import org.openremote.model.PersistenceEvent;
import org.openremote.model.asset.Asset;
import org.openremote.model.attribute.*;
import org.openremote.model.datapoint.*;
import org.openremote.model.datapoint.query.AssetDatapointAllQuery;
import org.openremote.model.query.AssetQuery;
import org.openremote.model.query.filter.AttributePredicate;
import org.openremote.model.query.filter.NameValuePredicate;
import org.openremote.model.query.filter.StringPredicate;
import org.openremote.model.value.AnomalyDetectionConfiguration;
import org.openremote.model.value.ForecastConfigurationWeightedExponentialAverage;
import org.openremote.model.value.MetaItemType;
import  org.openremote.model.attribute.AttributeAnomaly.AnomalyType;

import java.time.Duration;
import java.time.ZoneId;
import java.util.*;
import java.util.logging.Logger;

import static org.openremote.container.persistence.PersistenceService.PERSISTENCE_TOPIC;
import static org.openremote.container.persistence.PersistenceService.isPersistenceEventForEntityType;
import static org.openremote.manager.gateway.GatewayService.isNotForGateway;
import static org.openremote.model.attribute.Attribute.getAddedOrModifiedAttributes;
import static org.openremote.model.util.TextUtil.requireNonNullAndNonEmpty;
import static org.openremote.model.value.MetaItemType.*;

/**
 * Calculates forecast values for asset attributes with an attached {@link MetaItemType#FORECAST}
 * configuration like {@link ForecastConfigurationWeightedExponentialAverage}.
 */
public class AnomalyDetectionService extends RouteBuilder implements ContainerService, EventListener{

    private static final Logger LOG = Logger.getLogger(AnomalyDetectionService.class.getName());
    private static long STOP_TIMEOUT = Duration.ofSeconds(5).toMillis();
    private List<Asset<?>> anomalyDetectionAssets;
    private HashMap<String, AnomalyAttribute> anomalyDetectionAttributes;


    protected GatewayService gatewayService;
    protected AssetStorageService assetStorageService;
    protected ClientEventService clientEventService;
    protected AssetDatapointService assetDatapointService;
    protected AssetAnomalyDatapointService assetAnomalyDatapointService;

    @Override
    public void init(Container container) throws Exception {
        anomalyDetectionAssets = new ArrayList<>();
        anomalyDetectionAttributes = new HashMap<>();

        gatewayService = container.getService(GatewayService.class);
        assetStorageService = container.getService(AssetStorageService.class);
        clientEventService = container.getService(ClientEventService.class);
        assetDatapointService = container.getService(AssetDatapointService.class);
        assetAnomalyDatapointService = container.getService(AssetAnomalyDatapointService.class);
    }


    @Override
    public void start(Container container) throws Exception {
        container.getService(MessageBrokerService.class).getContext().addRoutes(this);
        clientEventService.addInternalSubscription(AttributeEvent.class,null, this::onAttributeChange );

        LOG.fine("Loading anomaly detection asset attributes...");

        anomalyDetectionAssets = getAnomalyDetectionAssets();

        anomalyDetectionAssets.forEach(asset -> asset.getAttributes().stream().filter(attr -> attr.hasMeta(ANOMALYDETECTION) && attr.hasMeta(STORE_DATA_POINTS)).forEach(
                attribute -> {
                    anomalyDetectionAttributes.put(asset.getId() + "$" + attribute.getName(),new AnomalyAttribute(asset,attribute));
                }
        ));



        LOG.fine("Found anomaly detection asset attributes count  = " + anomalyDetectionAttributes.size());
    }

    @Override
    public void stop(Container container) throws Exception {

    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure() throws Exception {
        from(PERSISTENCE_TOPIC)
                .routeId("Persistence-AnomalyDetectionConfiguration")
                .filter(isPersistenceEventForEntityType(Asset.class))
                .filter(isNotForGateway(gatewayService))
                .process(exchange -> {
                    PersistenceEvent<Asset<?>> persistenceEvent = (PersistenceEvent<Asset<?>>)exchange.getIn().getBody(PersistenceEvent.class);
                    processAssetChange(persistenceEvent);
                });
    }

    protected void processAssetChange(PersistenceEvent<Asset<?>> persistenceEvent) {

        LOG.finest("Processing asset persistence event: " + persistenceEvent.getCause());
        Asset<?> asset = persistenceEvent.getEntity();

        //updates hashmap with the attributes to contain only attributes with the anomaly detection meta item
        switch (persistenceEvent.getCause()) {
            case CREATE:
                // loop through all attributes with an anomaly detection meta item and add them to the watch list.
                (asset.getAttributes().stream().filter(attr -> attr.hasMeta(ANOMALYDETECTION) && attr.hasMeta(STORE_DATA_POINTS))).forEach(
                        attribute -> {
                                anomalyDetectionAttributes.put(asset.getId() + "$" + attribute.getName(),new AnomalyAttribute(asset,attribute));
                        }
                );
                break;
            case UPDATE:
                (((AttributeMap) persistenceEvent.getPreviousState("attributes")).stream().filter(attr -> attr.hasMeta(ANOMALYDETECTION))).forEach(
                        attribute -> {
                            anomalyDetectionAttributes.remove(asset.getId() + "$" + attribute.getName());
                        }
                );
                (asset.getAttributes().stream().filter(attr -> attr.hasMeta(ANOMALYDETECTION) && attr.hasMeta(STORE_DATA_POINTS))).forEach(
                        attribute -> {
                            if(attribute.hasMeta(STORE_DATA_POINTS)){
                                anomalyDetectionAttributes.put(asset.getId() + "$" + attribute.getName(),new AnomalyAttribute(asset,attribute));
                            }
                        }
                );
                break;
            case DELETE: {
                (((AttributeMap) persistenceEvent.getCurrentState("attributes")).stream().filter(attr -> attr.hasMeta(ANOMALYDETECTION))).forEach(
                        attribute -> {
                            anomalyDetectionAttributes.remove(asset.getId() + "$" + attribute.getName());
                        }
                );
                break;
            }
        }
    }

    protected List<Asset<?>> getAnomalyDetectionAssets() {
        return assetStorageService.findAll(
            new AssetQuery().attributes(
                new AttributePredicate().meta(
                    new NameValuePredicate(
                            ANOMALYDETECTION,
                        new StringPredicate(AssetQuery.Match.CONTAINS, true, "type")
                    )
                )
            )
        );
    }

    protected void onAttributeChange(AttributeEvent event) {
        //only handle events coming from attributes in the with anomaly detection
        AnomalyType anomalyType = AnomalyType.Unchecked;
         if(anomalyDetectionAttributes.containsKey(event.getAssetId() + "$" + event.getAttributeName())){
             AnomalyAttribute anomalyAttribute = anomalyDetectionAttributes.get(event.getAssetId() + "$" + event.getAttributeName());
             anomalyType = anomalyAttribute.validateDatapoint(event.getValue(), event.getTimestamp());
             assetAnomalyDatapointService.updateValue(anomalyAttribute.getId(), anomalyAttribute.getName(),anomalyType, event.timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime(), anomalyAttribute);
         }
    }

    public class AnomalyAttribute {
        private AttributeRef attributeRef;
        private List<DetectionMethod> detectionMethods;
        public boolean hasEnoughDatapoints;
        public AnomalyAttribute(Asset<?> asset, Attribute<?> attribute) {
            this(asset.getId(), attribute);
        }

        public AnomalyAttribute(String assetId, Attribute<?> attribute) {
            requireNonNullAndNonEmpty(assetId);
            if (attribute == null) {
                throw new IllegalArgumentException("Attribute cannot be null");
            }
            this.attributeRef = new AttributeRef(assetId, attribute.getName());
            this.detectionMethods = new ArrayList<>();
            hasEnoughDatapoints = false;
            List<AssetDatapoint> dtapoints = GetDatapoints();
            if(dtapoints.size() >1) hasEnoughDatapoints = true;
            for (AnomalyDetectionConfiguration con: attribute.getMetaValue(ANOMALYDETECTION).orElse(null)){
                switch (con.getClass().getSimpleName()) {
                    case "Global" -> detectionMethods.add(new DetectionMethodGlobal(con));
                    case "Change" -> detectionMethods.add(new DetectionMethodChange(con));
                }
            }
            for (DetectionMethod method: detectionMethods) {
                if(hasEnoughDatapoints) method.UpdateData(dtapoints);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnomalyDetectionService.AnomalyAttribute that = (AnomalyDetectionService.AnomalyAttribute) o;
            return attributeRef.getId().equals(that.attributeRef.getId()) && attributeRef.getName().equals(that.attributeRef.getName());
        }

        public String getId() {
            return attributeRef.getId();
        }

        public String getName() {
            return attributeRef.getName();
        }

        public AttributeRef getAttributeRef() {
            return attributeRef;
        }

        public AnomalyType validateDatapoint(Optional<Object> value, long timestamp){
            AnomalyType anomalyType = AnomalyType.Unchecked;
            int anomalyCount = 0;

            for (DetectionMethod method: detectionMethods) {
                if(value.isPresent() && method.config.onOff){
                    if(hasEnoughDatapoints){
                        if (!method.validateDatapoint(value.get(),timestamp)) {
                            anomalyCount++;
                            anomalyType = method.anomalyType;
                        }
                    }else{
                        anomalyCount = -1;
                        List<AssetDatapoint> datapoints = GetDatapoints();
                        if(datapoints.size() > 1){
                            method.UpdateData(datapoints);
                            hasEnoughDatapoints = true;
                        }
                    }
                    if(!method.checkRecentDataSaved(timestamp)){
                        List<AssetDatapoint> datapoints = GetDatapoints();
                        if(datapoints.size() > 1){
                            method.UpdateData(datapoints);
                            hasEnoughDatapoints = true;
                        }
                    }

                }
            }
            if(anomalyCount == 0) anomalyType = AnomalyType.Valid;
            if(anomalyCount > 1) anomalyType = AnomalyType.Multiple;

            return  anomalyType;
        }
        public List<AssetDatapoint> GetDatapoints(){
            DatapointPeriod period = assetDatapointService.getDatapointPeriod(attributeRef.getId(), attributeRef.getName());
            List<AssetDatapoint> datapoints = new ArrayList<>();
            if(period.getLatest() == null) return datapoints;
            long maxTimespan = 0;
            for (DetectionMethod detectionMethod: detectionMethods){
                if(detectionMethod.config.timespan.toMillis() > maxTimespan) maxTimespan = detectionMethod.config.timespan.toMillis();
            }
            //test if there are enough datapoints for all detection methods
            if(period.getLatest() - period.getOldest() < maxTimespan)return datapoints;
            for (ValueDatapoint<?> datapoint: assetDatapointService.queryDatapoints(attributeRef.getId(), attributeRef.getName(),new AssetDatapointAllQuery(period.getLatest()- maxTimespan, period.getLatest() ))) {
                datapoints.add(new AssetDatapoint(attributeRef,datapoint.getValue(),datapoint.getTimestamp()));
            }

            return datapoints;
        }
    }

    private abstract class DetectionMethod implements IDetectionMethod{
        public AnomalyDetectionConfiguration config;
        public AnomalyType anomalyType;
        public  DetectionMethod(AnomalyDetectionConfiguration config){
            this.config = config;
        }
    }
    private interface IDetectionMethod{
        /**Check if value is valid according to the methods rules. */
        boolean validateDatapoint(Object value, long timestamp);
        /**Update needsNewData based on needs method */
        boolean checkRecentDataSaved(long latestTimestamp);
        /**Update saved values used to calculate Limits */
        void UpdateData(List<AssetDatapoint> datapoints);
    }


    private class DetectionMethodGlobal extends DetectionMethod{
        private double minValue;
        private long minValueTimestamp;
        private double maxValue;
        private long maxValueTimestamp;


        public DetectionMethodGlobal(AnomalyDetectionConfiguration config){
            super(config);
            anomalyType = AnomalyType.GlobalOutlier;
        }


        public boolean validateDatapoint(Object value, long timestamp) {
            double differance = maxValue - minValue;
            double deviation = differance/2 * (1 + (double)config.deviation /100);

            double val = (double)value;
            boolean valid = true;
            if(val < minValue - deviation){
                minValue = val;
                minValueTimestamp = timestamp;
                valid = false;
            }
            if(val > maxValue + deviation){
                maxValue = val;
                maxValueTimestamp = timestamp;
                valid = false;
            }
            if(valid){
                if(val >= maxValue){
                    maxValue = val;
                    maxValueTimestamp = timestamp;
                }
                if(val <= minValue){
                    minValue = val;
                    minValueTimestamp =timestamp;
                }
            }
            return valid;
        }


        public boolean checkRecentDataSaved(long latestTimestamp) {
            boolean needsNewData = false;
            long timeMillis =config.timespan.toMillis();
            if(minValueTimestamp < latestTimestamp - timeMillis
            || maxValueTimestamp < latestTimestamp - timeMillis){
                needsNewData = true;
            }
            return !needsNewData;
        }

        @Override
        public void UpdateData(List<AssetDatapoint> datapoints) {
            minValue = Double.MAX_VALUE;
            maxValue = (double)datapoints.get(0).getValue();
            for (AssetDatapoint dtapoint : datapoints) {
                if((double)dtapoint.getValue() <= minValue){
                    minValue = (double)dtapoint.getValue();
                    minValueTimestamp = dtapoint.getTimestamp();
                }
                if((double)dtapoint.getValue() >= maxValue){
                    maxValue = (double)dtapoint.getValue();
                    maxValueTimestamp = dtapoint.getTimestamp();
                }
            }
        }
    }
    private class DetectionMethodChange extends DetectionMethod{

        double biggestIncrease;
        long biggestIncreaseTimestamp;
        double smallestIncrease;
        long smallestIncreaseTimestamp;
        double previousValue;
        long previousValueTimestamp;

        public DetectionMethodChange(AnomalyDetectionConfiguration config){
            super(config);
            anomalyType = AnomalyType.ContextualOutlier;
        }

        @Override
        public boolean validateDatapoint(Object value, long timestamp) {
            double increase = ((double)value - previousValue);

            boolean valid = true;
            double offset = 0;
            offset = biggestIncrease* ((double)config.deviation/100);
            if(offset < 0) offset *= -1;
            if(increase > biggestIncrease + offset){
                biggestIncrease = increase;
                biggestIncreaseTimestamp = timestamp;
                valid = false;
            }
            offset = smallestIncrease* ((double)config.deviation/100);
            if(offset < 0) offset *= -1;
            if(increase < smallestIncrease - offset){
                smallestIncrease = increase;
                smallestIncreaseTimestamp = timestamp;
                valid = false;
            }
            if(valid){
                if(increase <= smallestIncrease){
                    smallestIncrease = increase;
                    smallestIncreaseTimestamp = timestamp;
                }
                if(increase>= biggestIncrease){
                    biggestIncrease = increase;
                    biggestIncreaseTimestamp = timestamp;
                }
            }
            previousValue = (double)value;
            previousValueTimestamp = timestamp;
            return valid;
        }

        @Override
        public boolean checkRecentDataSaved(long latestTimestamp) {
            boolean needsNewData = false;
            if(smallestIncreaseTimestamp < latestTimestamp - config.timespan.toMillis()
                    || biggestIncreaseTimestamp < latestTimestamp - config.timespan.toMillis()){
                needsNewData = true;
            }
            return !needsNewData;
        }

        @Override
        public void UpdateData(List<AssetDatapoint> datapoints) {

            if(datapoints.size() <2) return;
            smallestIncrease = Double.MAX_VALUE;
            biggestIncrease = (double)datapoints.get(1).getValue() - (double)datapoints.get(0).getValue();
            for(int i = 1; i < datapoints.size(); i++){
               double increase = (double)datapoints.get(i).getValue() - (double)datapoints.get(i-1).getValue();
               long timestamp = datapoints.get(i).getTimestamp();

                if(increase <= smallestIncrease){
                    smallestIncrease = increase;
                    smallestIncreaseTimestamp = timestamp;
                }
                if(increase>= biggestIncrease){
                    biggestIncrease = increase;
                    biggestIncreaseTimestamp = timestamp;
                }
            }
            previousValue = (double)datapoints.get(0).getValue();
            previousValueTimestamp = datapoints.get(0).getTimestamp();

        }
    }
}

