/*
 * Copyright 2017, OpenRemote Inc.
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
package org.openremote.manager.datapoint;

import org.hibernate.Session;
import org.openremote.agent.protocol.ProtocolPredictedDatapointService;
import org.openremote.container.persistence.PersistenceService;
import org.openremote.container.timer.TimerService;
import org.openremote.manager.asset.AnomalyDetectionService;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.manager.security.ManagerIdentityService;
import org.openremote.manager.web.ManagerWebService;
import org.openremote.model.Container;
import org.openremote.model.ContainerService;
import org.openremote.model.attribute.AttributeAnomaly;
import org.openremote.model.attribute.AttributeRef;
import org.openremote.model.datapoint.AssetDatapoint;
import org.openremote.model.datapoint.AssetPredictedDatapoint;
import org.openremote.model.util.Pair;
import org.openremote.model.util.ValueUtil;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.time.temporal.ChronoUnit.HOURS;

public class AssetAnomalyDatapointService implements ContainerService {
    public static final int PRIORITY = AssetStorageService.PRIORITY + 100;
    private static final Logger LOG = Logger.getLogger(AssetAnomalyDatapointService.class.getName());

    protected PersistenceService persistenceService;
    protected AssetStorageService assetStorageService;
    protected TimerService timerService;
    protected ScheduledExecutorService executorService;
    protected ScheduledFuture<?> dataPointsPurgeScheduledFuture;


    @Override
    public int getPriority() {
        return ContainerService.super.getPriority();
    }

    public void init(Container container) throws Exception {
        persistenceService = container.getService(PersistenceService.class);
        assetStorageService = container.getService(AssetStorageService.class);
        timerService = container.getService(TimerService.class);
        executorService = container.getExecutorService();
    }


    public void start(Container container) throws Exception {
        dataPointsPurgeScheduledFuture = executorService.scheduleAtFixedRate(
            this::purgeDataPoints,
            timerService.getNow().toEpochMilli(),
            Duration.ofDays(1).toMillis(), TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void stop(Container container) throws Exception {
        if (dataPointsPurgeScheduledFuture != null) {
            dataPointsPurgeScheduledFuture.cancel(true);
        }
    }

    public void updateValue(String assetId, String attributeName, AttributeAnomaly.AnomalyType anomalyType, LocalDateTime timestamp, AnomalyDetectionService.AnomalyAttribute data) {
        persistenceService.doTransaction(em ->
                em.unwrap(Session.class).doWork(connection -> {

                    getLogger().finest("Storing anomaly datapoint for: id=" + assetId + ", name=" + attributeName + ", timestamp=" + timestamp + ", anomalyType=" + anomalyType);
                    PreparedStatement st;

                    try {
                        st = getUpsertPreparedStatement(connection);
                        setUpsertValues(st, assetId, attributeName, anomalyType.ordinal(), timestamp, data);
                        st.executeUpdate();
                    } catch (Exception e) {
                        String msg = "Failed to insert/update data point: ";
                        getLogger().log(Level.WARNING, msg, e);
                        throw new IllegalStateException(msg, e);
                    }
                }));
    }

    protected PreparedStatement getUpsertPreparedStatement(Connection connection) throws SQLException {
        return connection.prepareStatement("INSERT INTO " + getDatapointTableName() + " (entity_id, attribute_name, anomaly_type, timestamp, data) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON CONFLICT (entity_id, attribute_name, timestamp) DO UPDATE " +
                "SET anomaly_type = excluded.anomaly_type");
    }

    protected void setUpsertValues(PreparedStatement st, String assetId, String attributeName, Integer anomalyType, LocalDateTime timestamp, AnomalyDetectionService.AnomalyAttribute data) throws Exception {
        st.setString(1, assetId);
        st.setString(2, attributeName);
        st.setInt(3, anomalyType);
        st.setObject(4, timestamp);
        PGobject pgJsonValue = new PGobject();
        pgJsonValue.setType("jsonb");
        pgJsonValue.setValue(ValueUtil.asJSON(data).orElse("null"));
        st.setObject(5, pgJsonValue);
    }


    protected Class<AttributeAnomaly> getAttributeAnomalyClass() {
        return AttributeAnomaly.class;
    }

    protected String getDatapointTableName() {
        return AttributeAnomaly.TABLE_NAME;
    }


    protected Logger getLogger() {
        return LOG;
    }



    protected void purgeDataPoints() {
        try {
            // Purge data points not in the above list using default duration
            LOG.finest("Purging predicted data points older than now");

        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to run data points purge", e);
        }
    }
}
