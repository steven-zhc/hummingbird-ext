package com.hczhang.hummingbird.ext.eventstore.mysql;

import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.event.EventConstants;
import com.hczhang.hummingbird.eventsource.EventSourceException;
import com.hczhang.hummingbird.model.EventSourceAggregateRoot;
import com.hczhang.hummingbird.model.exception.ModelRuntimeException;
import com.hczhang.hummingbird.repository.AbstractEventSourceRepository;
import com.hczhang.hummingbird.repository.RepositoryRuntimeException;
import com.hczhang.hummingbird.serializer.JsonSerializer;
import com.hczhang.hummingbird.serializer.Serializer;
import com.hczhang.hummingbird.util.HBAssert;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by steven on 9/15/14.
 */
public class MysqlEventSourceRepository extends AbstractEventSourceRepository {

    private static Logger logger = LoggerFactory.getLogger(MysqlEventSourceRepository.class);

    private Serializer serializer = new JsonSerializer();

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void init(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    protected Queue<Event> queryEvents(Object aggregateID, long sinceVersion) {
        HBAssert.notNull(aggregateID, ModelRuntimeException.class, "AggregateID must not be null");

        String sql = "select * from events where aid = ? and version > ? order by version asc";

        List<Event> list = jdbcTemplate.query(sql, new EventRowMapper(), aggregateID.toString(), new Long(sinceVersion));

        return new LinkedList<Event>(list);
    }

    @Override
    protected Queue<Event> queryEvents(Object aggregateID, long startVersion, long endVersion) {
        return null;
    }

    @Override
    protected void saveEvents(final Queue<Event> eventStream) {
        Validate.notNull(eventStream, "EventStream must not be null");

        String sql = "insert into `events` (`aid`, `tid`, `body`, `ctime`, `version`, `mt`, `et`) values (?, ?, ?, ?, ?, ?, ?)";
        final int eventSize = eventStream.size();
        final Iterator<Event> it = eventStream.iterator();

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Event e = it.next();

                ps.setString(1, e.getAggregateID().toString());
                ps.setString(2, e.getCommandID());
                ps.setBytes(3, serializer.serialize(e));
                ps.setTimestamp(4, new Timestamp(e.getTimestamp()));
                ps.setLong(5, e.getVersion());
                ps.setString(6, e.getMetaData(EventConstants.META_MODEL_TYPE));
                ps.setString(7, e.getMetaData(EventConstants.META_EVENT_TYPE));
            }

            @Override
            public int getBatchSize() {
                return eventSize;
            }
        });
    }

    @Override
    protected void snapshot(EventSourceAggregateRoot model) {
        HBAssert.notNull(model, ModelRuntimeException.class, "Aggregate root must not be null");

        String sql = "insert into `snapshot` (`aid`, `mt` , `version` , `ctime`, `body` ) values (?, ?, ?, ?, ?)";

        jdbcTemplate.update(sql, model.getAggregateID().toString(), model.getClass().getName(),
                model.getVersion(), new java.util.Date(), serializer.serialize(model));
    }

    @Override
    public EventSourceAggregateRoot retrieveSnapshot(Object id) {
        HBAssert.notNull(id, ModelRuntimeException.class, "AggregateID must not be null");

        String sql = "select * from snapshot where aid = ? order by version desc limit 1";

        try {
            return jdbcTemplate.queryForObject(sql, new SnapshotRowMapper(), id.toString());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public EventSourceAggregateRoot retrieveSnapshot(Object aid, long version) {
        return null;
    }

    @Override
    public void dropSnapshot(Object aid, long version) {
        // TODO: finish this
    }

    @Override
    public long getModelVersion(Object aggregateID) {
        HBAssert.notNull(aggregateID, ModelRuntimeException.class, "AggregateID must not be null");

        String sql = "select max(version) from events where aid = ?";

        return jdbcTemplate.queryForLong(sql, aggregateID.toString());
    }

    @Override
    public void dropEvents(Object aid, long version) {
        // TODO: finish this
    }

    private class EventRowMapper implements RowMapper<Event> {

        @Override
        public Event mapRow(ResultSet rs, int rowNum) throws SQLException {

            byte[] body = rs.getBytes("body");
            String eventType = rs.getString("et");
            try {
                Class classType = Class.forName(eventType);
                Event event = (Event) serializer.deserialize(body, classType);

                return event;
            } catch (ClassNotFoundException e) {
                throw new EventSourceException("Cannot find class [{}]", eventType);
            }
        }
    }

    private class SnapshotRowMapper implements RowMapper<EventSourceAggregateRoot> {

        @Override
        public EventSourceAggregateRoot mapRow(ResultSet rs, int rowNum) throws SQLException {
            byte[] body = rs.getBytes("body");
            String modelType = rs.getString("mt");

            try {
                Class classType = Class.forName(modelType);
                EventSourceAggregateRoot model = (EventSourceAggregateRoot) serializer.deserialize(body, classType);

                return model;
            } catch (ClassNotFoundException e) {
                throw new EventSourceException("Cannot find class [{}]", modelType);
            } catch (Exception e) {
                logger.error("Cannot concrete snapshot. raw string: [{}]", new String(body), e);
                throw new RepositoryRuntimeException("Cannot take snapshot. Error<{}>:[{}]", e.getClass().getSimpleName(), e.getMessage());
            }
        }
    }
}
