import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonEOFException;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight MessagePack-based TCP input format that mirrors the behaviour of the connector
 * but handles end-of-stream gracefully for the DataStream integration.
 */
public class TcpRowInputFormat implements InputFormat<RowData, InputSplit> {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final RowType rowType;

    private transient Socket socket;
    private transient JsonParser parser;
    private transient MessagePackFactory factory;

    private transient boolean reachedEof;
    private transient int fieldCount;
    private transient List<LogicalTypeRoot> fieldTypes;

    public TcpRowInputFormat(String host, int port, RowType rowType) {
        this.host = host;
        this.port = port;
        this.rowType = rowType;
    }

    @Override
    public void open(InputSplit split) throws IOException {
        this.fieldCount = rowType.getFieldCount();
        this.fieldTypes = new ArrayList<>(fieldCount);
        rowType.getFields().forEach(f -> fieldTypes.add(f.getType().getTypeRoot()));

        factory = new MessagePackFactory();
        socket = new Socket(host, port);
        parser = factory.createParser(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        reachedEof = false;
    }

    @Override
    public boolean reachedEnd() {
        return reachedEof;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (reachedEof) {
            return null;
        }

        try {
            JsonToken token = parser.nextToken();
            if (token == null) {
                markEndOfStream();
                return null;
            }

            while (token != JsonToken.START_ARRAY) {
                token = parser.nextToken();
                if (token == null) {
                    markEndOfStream();
                    return null;
                }
            }

            GenericRowData row;
            if (reuse instanceof GenericRowData) {
                row = (GenericRowData) reuse;
                row.setRowKind(RowKind.INSERT);
            } else {
                row = new GenericRowData(RowKind.INSERT, fieldCount);
            }

            for (int i = 0; i < fieldCount; i++) {
                JsonToken valueToken = parser.nextToken();
                LogicalTypeRoot typeRoot = fieldTypes.get(i);

                if (valueToken == null || valueToken == JsonToken.VALUE_NULL) {
                    row.setField(i, null);
                    continue;
                }

                switch (typeRoot) {
                    case INTEGER:
                        row.setField(i, parser.getIntValue());
                        break;
                    case BIGINT:
                        row.setField(i, parser.getLongValue());
                        break;
                    case DOUBLE:
                        row.setField(i, parser.getDoubleValue());
                        break;
                    case FLOAT:
                        row.setField(i, (float) parser.getDoubleValue());
                        break;
                    case CHAR:
                    case VARCHAR:
                        row.setField(i, StringData.fromString(parser.getText()));
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        long millis = parser.getLongValue();
                        row.setField(i, TimestampData.fromEpochMillis(millis));
                        break;
                    default:
                        parser.skipChildren();
                        row.setField(i, null);
                        break;
                }
            }

            // consume END_ARRAY token
            parser.nextToken();
            return row;
        } catch (JsonEOFException eof) {
            markEndOfStream();
            return null;
        }
    }

    private void markEndOfStream() throws IOException {
        reachedEof = true;
        close();
    }

    @Override
    public void close() throws IOException {
        if (parser != null) {
            parser.close();
            parser = null;
        }
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public void configure(Configuration parameters) {
        // no-op
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return null;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] splits) {
        return new DefaultInputSplitAssigner(splits);
    }
}

