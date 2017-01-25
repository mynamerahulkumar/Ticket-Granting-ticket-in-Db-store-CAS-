package clari5.sso;

import clari5.platform.dbcon.DbTypeEnum;
import clari5.platform.dbcon.Pw;
import clari5.platform.exceptions.RuntimeFatalException;
import clari5.platform.util.Hocon;
import com.mchange.v2.c3p0.AbstractComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.Provider;
import java.security.Security;
import java.sql.Connection;
import java.util.Properties;

/**
 * Created by rahul on 1/10/17.
 */
public class CxComboPooledDataSource extends AbstractComboPooledDataSource {

    private DbTypeEnum dbType;
    private boolean autoCommit = false;
    private int isolationLevel = 2;
    private String driver;
    private String url;
    private String machine;
    private int port;
    private String sid;
    private String database;
    private String schema;
    private String user;
    private String passwd;

    CxComboPooledDataSource()  {
        super();
        Hocon poolCfg = new Hocon();
        try {
            poolCfg.loadFromContext("dbcon-module.conf");
            configure(poolCfg.get("dbcon"),null);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    public  void configure(Hocon cfg,Hocon fallback) throws PropertyVetoException {

        if(fallback == null) {
            fallback = cfg;
        }

        this.parse(cfg, fallback);
        this.setAutoCommitOnClose(false);
        this.setUser(this.user);
        this.setInitialPoolSize(this.hGetInt("pool.init-size", cfg, fallback, 1));
        this.setMinPoolSize(this.hGetInt("pool.min-size", cfg, fallback, 1));
        this.setMaxPoolSize(this.hGetInt("pool.max-size", cfg, fallback, 5));
        this.setMaxConnectionAge(this.hGetInt("pool.age.max", cfg, fallback, 0));
        this.setMaxIdleTime(this.hGetInt("pool.age.max-idle-time", cfg, fallback, 10000));
        this.setMaxIdleTimeExcessConnections(this.hGetInt("pool.age.max-idle-time-excess-connection", cfg, fallback, 0));
        this.setMaxStatements(this.hGetInt("pool.statement.max", cfg, fallback, 10));
        this.setMaxStatementsPerConnection(this.hGetInt("pool.statement.max-per-connection", cfg, fallback, 0));
        this.setCheckoutTimeout(this.hGetInt("pool.admin.checkout-time", cfg, fallback, 0));
        this.setNumHelperThreads(this.hGetInt("pool.admin.num-helper-threads", cfg, fallback, 2));
        this.setAcquireIncrement(1);
        this.setTestConnectionOnCheckin(false);
        this.setTestConnectionOnCheckout(false);

        if(this.dbType == DbTypeEnum.ORACLE) {
            String path = System.getProperty("CXPS_DEPLOYMENT");
            if(path == null) {
                path = System.getenv("CXPS_DEPLOYMENT");
            }

            if(path == null) {
                path = System.getenv("DEP_BASE");
            }

            if(path == null) {
                path = ".";
            }

            File sFile = new File(path + File.separator + cfg.getString("user").toUpperCase() + ".properties");
            if(sFile.exists()) {
                Properties props = new Properties();

                try {
                    props.load(new FileInputStream(sFile));
                    this.setProperties(props);

                    try {
                        this.setDriverClass(props.getProperty("driver"));
                    } catch (PropertyVetoException var9) {
                        throw new RuntimeFatalException.ResourceConfigError("DBCON", "Unable to set driver [" + props.getProperty("driver") + "]", var9);
                    }

                    this.setJdbcUrl(this.url = props.getProperty("url"));
                } catch (IOException var10) {
                    throw new RuntimeException("Unable to load property object from Certificate File", var10);
                }

                String pkiClass = "oracle.security.pki.OraclePKIProvider";

                try {
                    Class e = Class.forName(pkiClass);
                    Security.insertProviderAt((Provider)e.newInstance(), 3);
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException var8) {
                    throw new RuntimeException("Unable to locate class [" + pkiClass + "]", var8);
                }

                this.setUser(this.user);
                this.setPassword("");
                return;
            }
        }

        this.setUser(this.user);
        this.setJdbcUrl(this.url);
        this.setPassword(this.passwd);

    }

    private String hGetStr(String key, Hocon first, Hocon fallback, String defaultValue) {
        return first.getString(key, fallback.getString(key, defaultValue));
    }

    private int hGetInt(String key, Hocon first, Hocon fallback, int defaultValue) {
        return first.eval2Int(key, fallback.eval2Int(key, defaultValue));
    }

    private void parse(Hocon cfg, Hocon fallback){

        if((user = cfg.getString("user", null)) == null){
            throw new RuntimeFatalException.ResourceConfigError("DBCON: DB user not found");
        }

        // Deduce URL from driver name
        url = cfg.getString("url", null);
        driver = cfg.getString("driver", null);
        if(driver != null && url == null) driver = null;    // Ignore the passed value of driver
        if(driver == null){
            if(url == null){
                machine = cfg.getString("machine", null);
                port = cfg.getInt("port");
                sid = cfg.getString("sid", null);
                database = cfg.getString("database", user);
            }
        }
        if (schema == null) {
            schema = cfg.getString("schema", "DB");
        }

        switch(hGetStr("db-type", cfg, fallback, "").toLowerCase()){
            case "oracle":
                if(driver == null) driver = "oracle.jdbc.OracleDriver";
                if(url == null) url = "jdbc:oracle:thin:@"+machine+":"+port+":"+sid;
                dbType = DbTypeEnum.ORACLE;
                break;
            case "sqlserver":
                if(driver == null) driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                if(url == null) url = "jdbc:sqlserver://"+machine+":"+port+";database="+database+";sendStringParametersAsUnicode=false";
                dbType = DbTypeEnum.SQLSERVER;
                break;
            case "postgresql":
                if(driver == null) driver = "org.postgresql.Driver";
                if(url == null) url = "jdbc:postgresql://"+machine+":"+port+"/"+sid;
                dbType = DbTypeEnum.POSTGRESQL;
                break;
            case "db2":
                if(driver == null) driver = "com.ibm.db2.jcc.DB2Driver";
                if(url == null) url = "jdbc:db2://"+machine+":"+port+"/"+sid;
                dbType = DbTypeEnum.DB2;
                break;
            case "mysql":
                if(driver == null) driver = "com.mysql.jdbc.Driver";
                if(url == null) url = "jdbc:mysql://"+machine+":"+port+"/"+sid;
                dbType = DbTypeEnum.MYSQL;
                break;
            case "hsqldb":
                if(driver == null) driver = "org.hsqldb.jdbc.JDBCDriver";
                if(url == null) url = "jdbc:hsqldb:file:"+machine+";hsqldb.lock_file=false;hsqldb.sqllog=1-3;";
                dbType = DbTypeEnum.HSQLDB;
                break;
            default:
                throw new RuntimeFatalException.ResourceConfigError("DBCON", "DBCON: Invalid DB type [" + cfg.getString("db-type", "Not found") + "]");
        }

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String isolation = hGetStr("isolation", cfg, fallback, "COMMITTED").toUpperCase();
        switch(isolation){
            case "REPEATABLE_READ" : this.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ; break;
            case "SERIALIZABLE" : this.isolationLevel = Connection.TRANSACTION_SERIALIZABLE; break;
            default : this.isolationLevel = Connection.TRANSACTION_READ_COMMITTED; break;
        }
        autoCommit = cfg.getBoolean("auto-commit", fallback.getBoolean("auto-commit", false));

        String seed = cfg.getString("pmseed", null);
        String pmtype = cfg.getString("pmtype", null);
        try {
            if (seed == null) passwd = Pw.getPw(schema);
            else passwd = Pw.getPw(seed, pmtype);
        } catch (Throwable e) {
            throw new RuntimeFatalException("Unable to extract password", e);
        }
    }
}
