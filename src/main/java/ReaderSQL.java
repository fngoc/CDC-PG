import decoding.Decode;
import org.json.JSONObject;
import org.postgresql.replication.PGReplicationStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReaderSQL {

    private static final Logger logger = Logger.getLogger(ReaderSQL.class.getName());

    public void reedToFile(ConnectionManager connectionManager) throws SQLException, IOException, ParseException {
        File outputFile = new File(connectionManager.getPathFile());
        PGReplicationStream stream = connectionManager.getReplConnection().getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(connectionManager.getReplicaName())
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", connectionManager.getPublicationName())
                .start();

        Decode decode = new Decode();
        decode.loadDataTypes(connectionManager.getSqlConnection());

        while (true) {
            ByteBuffer buffer = stream.readPending();
            if (buffer == null)
                continue;
            HashMap<String, Object> message = decode.decodeLogicalReplicationMessage(buffer, false);
            String jsonString = new JSONObject(message).toString(2);
            writeInFile(outputFile, jsonString);
            if (connectionManager.isFlagKafkaConnector())
                sendRequestToKafkaConnector(jsonString, connectionManager.getTopic(), connectionManager.getBootstrapAddress());
            // Replication feedback
            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());
        }
    }

    private void writeInFile(File file, String text) throws IOException {
        logger.log(Level.INFO, text);
        FileWriter fileWriter = new FileWriter(file, true);
        fileWriter.write(text);
        fileWriter.write('\n');
        fileWriter.close();
    }

    private void sendRequestToKafkaConnector(String message, String topicName, String bootstrapAddress) {
        JSONObject jsonMessage= new JSONObject(message);
        KafkaMessageProducer reportObj = new KafkaMessageProducer();
        reportObj.send(topicName, bootstrapAddress, jsonMessage.toString());
    }
}
