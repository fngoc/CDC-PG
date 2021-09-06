import com.beust.jcommander.JCommander;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Args arguments = new Args();
        try {
            JCommander.newBuilder().addObject(arguments).build().parse(args);
        } catch (RuntimeException runtimeException) {
            System.out.println("Invalid login details, example: ip:port login password [-n name_replica] [-f file_path]");
            System.exit(1);
        }
        try {
            File file = new File(arguments.getPath());
            Properties props = new Properties();
            PGProperty.USER.set(props, arguments.getLogin());
            PGProperty.PASSWORD.set(props, arguments.getPassword());
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "13.4");
            PGProperty.REPLICATION.set(props, "database");
            PGProperty.PREFER_QUERY_MODE.set(props, "simple");

            Connection connection = DriverManager.getConnection("jdbc:postgresql://" +
                    arguments.getIp() + ":" + arguments.getPort() + "/postgres", props);
            PGConnection replConnection = connection.unwrap(PGConnection.class);

            String replicaName = "replica";
            try {
                if (arguments.getNameReplica() != null) {
                    try {
                        reedReplica(replConnection, arguments.getNameReplica(), file);
                    } catch (SQLException sqlException) {
                        createReplicaSlot(replConnection, arguments.getNameReplica());
                        reedReplica(replConnection, arguments.getNameReplica(), file);
                    }
                } else
                    createReplicaSlot(replConnection, replicaName);
            } catch (Exception exception) {
                System.out.println("A slot with this name already exists, so the default replica name was chosen [" + replicaName + "]");
                reedReplica(replConnection, replicaName, file);
            }
        } catch (SQLException sqlException) {
            System.out.println("Something went wrong with the SQL query");
        } catch (IOException ioException) {
            System.out.println("Problem creating or writing to file");
        }
    }

    private static void reedReplica(PGConnection replConnection, String replicaName, File file) throws SQLException, IOException {
        PGReplicationStream stream = replConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(replicaName)
                .withSlotOption("include-xids", true)
                .withSlotOption("skip-empty-xacts", true)
                .start();

        while (true) {
            ByteBuffer msg = stream.readPending();
            if (msg == null)
                continue;
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            String text = new String(source, offset, length);
            System.out.println(text);
            FileWriter fileWriter = new FileWriter(file, true);
            fileWriter.write(text);
            fileWriter.write('\n');
            fileWriter.close();
        }
    }

    private static void createReplicaSlot(PGConnection replConnection, String replicaName) throws SQLException {
        replConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(replicaName)
                .withOutputPlugin("test_decoding")
                .make();
    }
}
