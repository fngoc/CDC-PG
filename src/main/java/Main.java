import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) {
        Args arguments = new Args();
        try {
            JCommander.newBuilder().addObject(arguments).build().parse(args);
        } catch (ParameterException parameterException) {
            System.out.println("Invalid login details, example: -i ip -p port -u user_name " +
                    "-w password -d database_name [-r replica_slot_name] [-f file_path]");
            System.exit(0);
        }
        try {
            DriverConnect.setProperties(arguments);
            DriverConnect.run();
        } catch (SQLException sqlException) {
            System.out.println("Make sure you have the wal2json plugin installed:\n" +
                    "You can install it with [apt-get install postgresql-13-wal2json] command.\n" +
                    "Also make sure that the following values are in the PostgreSQL configuration file:\n" +
                    "max_wal_senders = 4 - greater than zero\n" +
                    "wal_level = logical\n" +
                    "max_replication_slots = 4 - greater than zero\n" +
                    "Make sure you are not overshot the replication slot limit.");
        } catch (IOException ioException) {
            System.out.println("Problem creating or writing to file");
        }
    }
}
