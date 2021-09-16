import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;
import org.postgresql.replication.PGReplicationStream;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;

public class ReaderSQL {

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
                sendRequestToKafkaConnector(jsonString);
            // Replication feedback
            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());
        }
    }

    private void writeInFile(File file, String text) throws IOException {
        System.out.println(text);
        FileWriter fileWriter = new FileWriter(file, true);
        fileWriter.write(text);
        fileWriter.write('\n');
        fileWriter.close();
    }

    private void sendRequestToKafkaConnector(String message) throws IOException {
        JSONObject jsonObject= new JSONObject(message);
        byte[] bytesEncoded = Base64.encodeBase64(jsonObject.toString().getBytes());

        String url = "http://localhost:8080/publish?message=" + new String(bytesEncoded);
        URL obj = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
        connection.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
    }
}
