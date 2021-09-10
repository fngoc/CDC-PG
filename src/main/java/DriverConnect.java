import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverConnect {

    private static PGConnection replConnection;
    private static String replicaName;
    private static String pathFile;

    private static boolean flagKafkaConnector;

    public static void setProperties(Args args) throws SQLException {
        flagKafkaConnector = args.isFlagKafkaConnector();
        Properties props = new Properties();
        PGProperty.USER.set(props, args.getLogin());
        PGProperty.PASSWORD.set(props, args.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "13.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        replicaName = args.getNameReplica() != null ? args.getNameReplica() : "replica_slot";
        pathFile = args.getPath() != null ? args.getPath() : replicaName + ".json";

        Connection connection = DriverManager.getConnection("jdbc:postgresql://" +
                args.getIp() + ":" + args.getPort() + "/" + args.getDatabase(), props);

        replConnection = connection.unwrap(PGConnection.class);
    }

    public static void run() throws SQLException, IOException {
        createReplicationSlot();
        reedReplicationSlot();
    }

    private static void reedReplicationSlot() throws SQLException, IOException {
        File file = new File(pathFile);
        PGReplicationStream stream = replConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(replicaName)
                .withSlotOption("include-xids", true)
                .withSlotOption("pretty-print", true)
                .start();

        while (true) {
            ByteBuffer msg = stream.readPending();
            if (msg == null)
                continue;
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            String text = new String(source, offset, length);
            writeInFile(file, text);
            if (flagKafkaConnector)
                sendRequestToKafkaConnector(text);
        }
    }

    private static void sendRequestToKafkaConnector(String message) throws IOException {
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

    private static void writeInFile(File file, String text) throws IOException {
//        System.out.println(text);
        FileWriter fileWriter = new FileWriter(file, true);
        fileWriter.write(text);
        fileWriter.write('\n');
        fileWriter.close();
    }

    private static void createReplicationSlot() {
        if (replicaName.equals("replica_slot"))
            System.out.println("A slot with this name already exists, default replica name is chosen [replica_slot]");
        try {
            replConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(replicaName)
                    .withOutputPlugin("wal2json")
                    .make();
            System.out.println("New replication slot created");
        }
        catch (SQLException sqlException) {
            System.out.println("Connected to an existing replication slot");
        }
    }
}
