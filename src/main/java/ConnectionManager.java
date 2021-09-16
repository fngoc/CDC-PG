import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

    private static PGConnection replConnection;

    private static Connection sqlConnection;
    private static String replicaName;
    private static String publicationName;
    private static String pathFile;

    private static boolean flagKafkaConnector;

    public ConnectionManager(Args args) throws Exception {
        setProperties(args);
    }

    public static void setProperties(Args args) throws Exception {
        flagKafkaConnector = args.isFlagKafkaConnector();
        Properties props = new Properties();
        PGProperty.USER.set(props, args.getLogin());
        PGProperty.PASSWORD.set(props, args.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "13.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        replicaName = args.getNameReplica() != null ? args.getNameReplica() : "replica_slot";
        pathFile = args.getPath() != null ? args.getPath() : replicaName + ".json";
        publicationName = args.getPublication();

        createSQLConnection(args, props);
        createReplicationSlot();
    }

    private static void createSQLConnection(Args args, Properties props) throws Exception {
        String url = "jdbc:postgresql://" + args.getIp() + ":" + args.getPort() + "/" + args.getDatabase();
        Class.forName("org.postgresql.Driver");
        sqlConnection = DriverManager.getConnection(url, props);
        replConnection = sqlConnection.unwrap(PGConnection.class);
        sqlConnection = DriverManager.getConnection(url, props);
        sqlConnection.setAutoCommit(true);
    }

    private static void createReplicationSlot() {
        if (replicaName.equals("replica_slot"))
            System.out.println("A slot with this name already exists, default replica name is chosen [replica_slot]");
        try {
            replConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(replicaName)
                    .withOutputPlugin("pgoutput")
                    .make();
            System.out.println("New replication slot created");
        }
        catch (SQLException sqlException) {
            System.out.println("Connected to an existing replication slot");
        }
    }

    public PGConnection getReplConnection() {
        return replConnection;
    }

    public Connection getSqlConnection() {
        return sqlConnection;
    }

    public String getReplicaName() {
        return replicaName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public String getPathFile() {
        return pathFile;
    }

    public boolean isFlagKafkaConnector() {
        return flagKafkaConnector;
    }
}
