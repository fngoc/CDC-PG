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
            System.out.println("Something went wrong with the SQL query or replication slot");
        } catch (IOException ioException) {
            System.out.println("Problem creating or writing to file");
        }
    }
}
