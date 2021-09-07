import com.beust.jcommander.Parameter;

public class Args {

    @Parameter(names = "-i", arity = 1, description = "Ip", required = true)
    private String ip;

    @Parameter(names = {"-port", "-p"}, arity = 1, description = "Port", required = true)
    private String port;

    @Parameter(names = {"-user", "-u"}, arity = 1, description = "Login", required = true)
    private String login;

    @Parameter(names = {"-password", "-w"}, arity = 1, description = "Password", required = true)
    private String password;

    @Parameter(names = {"-database", "-d"}, arity = 1, description = "Database", required = true)
    private String database;

    @Parameter(names = {"-replica", "-r"}, arity = 1, description = "Name replica")
    private String nameReplica = null;

    @Parameter(names = {"-file", "-f"}, arity = 1, description = "Path file")
    private String path = null;

    public String getIp() { return ip; }

    public String getPort() { return port; }

    public String getLogin() { return login; }

    public String getPassword() { return password; }

    public String getDatabase() { return database; }

    public String getNameReplica() { return nameReplica; }

    public String getPath() { return path; }
}
