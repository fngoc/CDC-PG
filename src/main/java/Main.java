import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

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
            ConnectionManager connectionManager = new ConnectionManager(arguments);
            ReaderSQL readerSQL = new ReaderSQL();
            readerSQL.reedToFile(connectionManager);
        } catch (SQLException e) {
            System.out.println(
                "1. You should have created a publication for the tables from which you want to read the changes." +
                "2. Also make sure that the following values are in the PostgreSQL configuration file:\n" +
                "max_wal_senders = 4 - greater than zero\n" +
                "wal_level = logical\n" +
                "max_replication_slots = 4 - greater than zero\n\n" +
                "3. Make sure you are not overshot the replication slot limit."
            );
        } catch (IOException e) {
            System.out.println("Problem creating or writing to file");
        } catch (ParseException e) {
            System.out.println("An error occurred while decoding");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
