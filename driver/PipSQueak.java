import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/* Not implemented: Meant to allow special command lines to be executed in event of exception
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
*/
import org.apache.drill.jdbc.DrillResultSet;
import org.json.simple.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * 
 */
/**
 * @author kkhatua
 *
 */
public class PipSQueak {
	private static final String DRILL_JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
	private static final String EOL = "\n";
	private static final String DELIMITER = "|";
	public static final String PRE_CANCEL = "preCancel";
	//	private static Logger log = LoggerFactory.getLogger(PipSQueak.class);
	private static String queryFileName;
	private String driverClass;
	private String query;
	private long registerDriverTime;
	private long loadingQueryTime;
	private long connectTime;
	private long executeTime;
	private long fetchFirstDone;
	private long fetchAllRowsTime;
	private long disconnectTime;
	private String connURL;
	private String userName;
	private String userPassword;
	private Connection connection;
	private Statement statement;
	private ResultSet resultSet;
	private long prepStmtTime;
	private long rowsRead;
	private File queryFile;
	private long alterSessionTime;
	private File alterationsFile;
	private int timeout;
	private boolean skipRowFetchDueToFailure;
	private String explainPlan;
	private Boolean showResults = false;
	private String explainPlanPrefix = "";
	private QueryTimer queryTimer;
	private boolean isCancelled = false;
	private String outputFileName;
	private PrintStream outputStream;
	private String explainVerbosePlanPrefix;
	private long getQueryIdTime;
	private HashMap<String, PipSQream> externalScriptRegistry;
	private String execCmdFileName;

	private void runTest() throws SQLException, IOException, ClassNotFoundException {
		/* All Steps are Timed!
		 * 1. Load Driver
		 * 2. Load Query from File
		 * 2. Connect to DB
		 * 3. Execute Query
		 * 4. Fetch Rows (Time 1st Row)
		 * 5. Close Connection
		 */
		loadQuery();
		System.out.println("Running Query:: " + this.queryFile.getName());
		System.out.flush();
		registerDriver();
		connectToDB();

		alterSession();

		prepareStmt();
		executeQuery();
		if (explainPlan == null) //Dont need Query IDs for ExplainPlans
			getQueryID();

		if (explainPlan != null)
			showPlan();
		else if (showResults)
			displayRows();
		else
			fetchRows();
		disconnect();
		printSummary();
	}

	private String getQueryID() {
		String queryId = null;
		long startTime = System.currentTimeMillis();
		//Incompatible with Impala: if (this.resultSet instanceof DrillResultSet) {
		if (this.driverClass.equalsIgnoreCase(DRILL_JDBC_DRIVER)) {
			DrillResultSet rs = (DrillResultSet)this.resultSet;
			if (rs != null) {
				try {
					queryId = ((DrillResultSet)this.resultSet).getQueryId();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (queryId != null) {
					System.out.println("[QUERYID] "+queryId);
					System.out.flush();
				}
			}
		}
		getQueryIdTime = System.currentTimeMillis() - startTime;
		return queryId;
	}

	private void displayRows() throws SQLException {
		rowsRead = 0L;
		if (skipRowFetchDueToFailure) {
			System.err.println("[ERROR] Skipping fetch() due to execute() failure");
			System.err.flush();
			return;
		}

		int columnCount = this.resultSet.getMetaData().getColumnCount();

		System.out.println("[INFO] Displaying rows for "+ columnCount +" columns...");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		@SuppressWarnings("unused")
		String dummyColValue;
		//Fetching 1st n-rows
		try {
			boolean isFirst = true;
			//Fetching remaining rows
			while (this.resultSet.next()) {
				if (isFirst) {
					//Fetching and Clocking 1st Row
					fetchFirstDone = System.currentTimeMillis() - startTime;
					isFirst = false;
				}
				for (int i = 1; i <= columnCount; i++) {
					outputStream.print(resultSet.getString(i)+ ( i == columnCount ? EOL : DELIMITER));
				}
				rowsRead++;		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (!outputFileName.equalsIgnoreCase("screen"))
				outputStream.close();
		}
		fetchAllRowsTime = System.currentTimeMillis() - startTime;	
	}

	private void alterSession() throws IOException, SQLException {
		System.out.println("[INFO] Altering Session");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		if (alterationsFile == null) {
			alterSessionTime = 0;
			return;
		}
		BufferedReader fileRdr = new BufferedReader(new FileReader(alterationsFile));
		String lineRead = null;
		while ((lineRead=fileRdr.readLine()) != null) {
			if (!lineRead.startsWith("--") && lineRead.trim().length() > 0) {
				if (lineRead.trim().endsWith(";"))
					lineRead = lineRead.replaceFirst(";", " ");

				Statement alterStmt = connection.createStatement();
				System.out.println("[WARN] Applying Alteration: " + lineRead);
				alterStmt.execute(lineRead);
				alterStmt.close();
			}
		}
		fileRdr.close();
		alterSessionTime = (System.currentTimeMillis() - startTime);
	}

	private void printSummary() {
		System.out.println("[STAT] Query : \n\t" + query);
		System.out.println("[STAT] Rows Fetched : " + rowsRead);
		System.out.println("[STAT] Time to load queries : " + loadingQueryTime + " msec" );
		System.out.println("[STAT] Time to register Driver : " + registerDriverTime + " msec");
		System.out.println("[STAT] Time to connect : " + connectTime + " msec");
		System.out.println("[STAT] Time to alter session : " + alterSessionTime + " msec");
		System.out.println("[STAT] Time to prep Statement  : " + prepStmtTime + " msec");
		System.out.println("[STAT] Time to execute query : " + executeTime + " msec");
		System.out.println("[STAT] Time to get query ID : " + getQueryIdTime + " msec");
		System.out.println("[STAT] Time to fetch 1st Row : " + fetchFirstDone + " msec");
		System.out.println("[STAT] Time to fetch All Rows : " + fetchAllRowsTime + " msec");
		System.out.println("[STAT] Time to disconnect : " + disconnectTime + " msec");
		System.out.println("[STAT] TOTAL TIME : " + (executeTime+fetchAllRowsTime) + " msec");
		System.out.flush();
	}

	private void connectToDB() throws SQLException {
		System.out.println("[INFO] Connecting to DB");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		connection = DriverManager.getConnection(
				this.connURL,
				this.userName, this.userPassword);
		connectTime = System.currentTimeMillis() - startTime;
	}

	private void loadQuery() throws IOException {
		System.out.println("[INFO] Loading query from file: " + queryFile.getName());
		System.out.flush();
		long startTime = System.currentTimeMillis();
		StringBuilder tmpQuery = new StringBuilder();
		BufferedReader fileRdr = new BufferedReader(new FileReader(queryFile));
		String lineRead = null;
		while ((lineRead=fileRdr.readLine()) != null) {
			if (!lineRead.startsWith("--")) {
				if (lineRead.trim().endsWith(";"))
					lineRead = lineRead.replaceFirst(";", " ");
				tmpQuery.append(lineRead.trim()+ " ");
			}
		}
		fileRdr.close();
		this.query = tmpQuery.toString();
		if (explainPlan != null) {
			if (explainPlan.equalsIgnoreCase("verbose")) 
				this.query = explainVerbosePlanPrefix  + tmpQuery.toString();
			else
				this.query = explainPlanPrefix  + tmpQuery.toString();
		}
		//"select * from region_par100;";
		loadingQueryTime = System.currentTimeMillis() - startTime;
	}

	private void prepareStmt() {
		System.out.println("[INFO] Preparing Statement");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		try {
			statement = connection.createStatement();
			try {
				//[TEMP]Until DRILL-2961 is resolved
				if (this.driverClass.equalsIgnoreCase(DRILL_JDBC_DRIVER)) 
					throw new SQLException("Method is not supported");					
				statement.setQueryTimeout(timeout);
			} catch (SQLException noTimeout) {
				noTimeout.printStackTrace(System.err);
				queryTimer = new QueryTimer();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		prepStmtTime = System.currentTimeMillis() - startTime;
	}

	private void executeQuery() {
		skipRowFetchDueToFailure = false;
		if (queryTimer != null)
			queryTimer.start();
		System.out.println("[INFO] Executing query...");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		try {
			resultSet = statement.executeQuery(query);
		} catch (SQLException e) {
			skipRowFetchDueToFailure = true;
			e.printStackTrace();
			System.err.println("[ERROR] Unable to execute " + retrieveCause(e));
			System.err.flush();
		}
		executeTime = System.currentTimeMillis() - startTime;
	}

	private void showPlan() {
		rowsRead = 0L;
		if (skipRowFetchDueToFailure) return;
		System.out.println("[INFO] So the plan is...");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		@SuppressWarnings("unused")
		String dummyColValue;

		//Fetching 1st n-rows
		try {
			//Fetching and Clocking 1st Row
			this.resultSet.next();
			System.out.println(this.resultSet.getString(1));
			System.out.flush();
			fetchFirstDone = System.currentTimeMillis() - startTime;
			rowsRead++;

			//Fetching remaining rows
			while (this.resultSet.next()) {
				System.out.println(this.resultSet.getString(1));
				System.out.flush();
				rowsRead++;		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		fetchAllRowsTime = System.currentTimeMillis() - startTime;
	}

	private void fetchRows() {
		rowsRead = 0L;
		if (skipRowFetchDueToFailure) {
			System.err.println("[ERROR] Skipping fetch() due to execute() failure");
			System.err.flush();
			return;
		}
		System.out.println("[INFO] Fetching rows...");
		System.out.flush();
		long startTime = System.currentTimeMillis();
		@SuppressWarnings("unused")
		String dummyColValue;
		//Fetching 1st n-rows
		try {
			//Fetching and Clocking 1st Row
			this.resultSet.next();
			fetchFirstDone = System.currentTimeMillis() - startTime;
			rowsRead++;
			//Fetching remaining rows
			while (this.resultSet.next()) {
				rowsRead++;		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.err.println("[ERROR] Unable to fetch all rows (got only "+rowsRead+") " + retrieveCause(e));
			System.err.flush();
			return;
		} finally {
			fetchAllRowsTime = System.currentTimeMillis() - startTime;
		}
	}

	private void disconnect() {
		long startTime = System.currentTimeMillis();
		//Closing ResultSet 
		try {
			if (this.resultSet != null ) {
				this.resultSet.close();
				System.out.println("[INFO] Releasing ResultSet resources");
				System.out.flush();
			}
		}
		catch (SQLException sqlExcp) {	 
			System.out.println("[FATAL (DATABASE : )]: "+sqlExcp.getLocalizedMessage());
			sqlExcp.printStackTrace(); //[NotExitingForNow] System.exit(2);
		}
		//Closing SQLStatement Object 
		try {
			if (this.statement != null ) {
				if (!this.statement.isClosed())
					this.statement.close();
				System.out.println("[INFO] Releasing JDBC (Statement) resources");
				System.out.flush();
			}	
		} catch (SQLException sqlExcp) {	 
			System.out.println("[FATAL (DATABASE : )]: "+sqlExcp.getLocalizedMessage());
			sqlExcp.printStackTrace(); //[NotExitingForNow] System.exit(2);
		}
		//Closing Connection Handle 
		try {
			if (this.connection != null ) {
				this.connection.close();
				System.out.println("[INFO] Closed connection ");
				System.out.flush();
			}	
		} catch (SQLException sqlExcp) {	 
			System.out.println("[FATAL (DATABASE : )]: "+sqlExcp.getLocalizedMessage());
			sqlExcp.printStackTrace(); //[NotExitingForNow] System.exit(2);
		}
		disconnectTime = (System.currentTimeMillis() - startTime);
	}

	//Retrieves the Root Cause in the stackTrace
	private String retrieveCause(SQLException e) {
		String causeText = "";
		Throwable cause = e;
		while(cause != null) {
			causeText = cause.toString();
			cause = cause.getCause();
		}
		return causeText;
	}
	public PipSQueak() throws FileNotFoundException {
		queryFile = new File(queryFileName); 
		if (!queryFile.canRead()) 
			throw new FileNotFoundException(queryFileName);
		this.driverClass = System.getProperty("driver", DRILL_JDBC_DRIVER);
		this.connURL = System.getProperty("conn", "jdbc:drill:schema=dfs.parquet;zk=localhost:5181");
		System.out.println("[INFO] Connection: " + connURL);
		if (connURL.contains("drill")) {
			explainVerbosePlanPrefix = "explain plan including all attributes for ";
			explainPlanPrefix = "explain plan for ";
		}
		//		else 
		//			explainPlanPrefix = "explain "; 
		this.userName = System.getProperty("user", "admin");
		this.userPassword = System.getProperty("password", "admin");
		this.timeout = Integer.valueOf(System.getProperty("timeout", "10"));
		this.explainPlan = System.getProperty("explain", null);
		this.outputFileName = System.getProperty("output");
		if (outputFileName != null) {
			this.showResults = true;
			if (outputFileName.equalsIgnoreCase("screen")) 
				outputStream = System.out;
			else {
				outputStream = new PrintStream(new FileOutputStream(outputFileName), true);
			}	
		}
		System.out.println("[WARN] Setting timeout as "+timeout+"sec");
		System.out.flush();
		alterationsFile = new File(System.getProperty("alter", ""));
		if (!alterationsFile.canRead())
			alterationsFile = null;

		//FIXME: Temp PreCancel Steps
		execCmdFileName = System.getProperty("prepost", null);
		if (execCmdFileName != null) {
			loadCommands(execCmdFileName);			
		}

	}
	/**
	 * Registers the driver for Database access
	 * @throws ClassNotFoundException 
	 */
	public void registerDriver() throws ClassNotFoundException {
		long startTime = System.currentTimeMillis();
		Class.forName(this.driverClass); //.newInstance();
		System.out.println("[INFO] Registered the database driver! ["+this.driverClass+"]");
		System.out.flush();
		registerDriverTime = System.currentTimeMillis() - startTime;
	}

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args)  {
		//Speed up on SysOut
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));

		try {
			PipSQueak.queryFileName = args[0];
			PipSQueak pipSQueak;
			pipSQueak = new PipSQueak();
			pipSQueak.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}	
	//*
	class QueryTimer extends Thread {
		public QueryTimer() {
			isCancelled = false;
			System.out.println("[INFO] Using QueryTimer thread to timeout in "+timeout+" sec");
			System.out.flush();
		}
		@Override
		public void run() {
			try {
				sleep(timeout*1000L);
				System.err.println("[TIME OUT] Query took more than "+timeout+" sec.");
				System.out.flush();
				cancelQuery();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			isCancelled = true;
		}

		public void cancelQuery() {
			try {
				//Performing PreCancellation Tasks
				executeScript(PRE_CANCEL);
				//Cancelling Query
				statement.cancel();
				if (!statement.isClosed())
					statement.close();		
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Execute command from Script Registry 
	 */
	private void executeScript(String commandRef) {
		PipSQream pipSQream = externalScriptRegistry.get(commandRef);
		if (pipSQream == null) {
			System.out.println("[WARN] No "+commandRef+" steps were defined");
			System.out.flush();
			return;
		}
		//System.out.println("[WARN] Executing "+ commandRef);		
		//executeShellCommand(pipSQream);
	}

	//Derive and eecute Shell Command defined
	/*
	private int executeShellCommand(PipSQream pipSQream2execute) {
		String command = pipSQream2execute.getCommand();
		File workingDir = ( pipSQream2execute.getExecDir() == null ? null : new File(pipSQream2execute.getExecDir())); 
		Long timeout = pipSQream2execute.getTimeout();
		int exitValue = -1;
		try {
			//Wait default 1mins for Completion 
			ExecuteWatchdog watchdog = new ExecuteWatchdog(timeout == null? 60000L : timeout);
			
			//Building command line
			CommandLine cmdLine = null;
			if (System.getProperty("os.name").startsWith("Windows")) {
				cmdLine = new CommandLine("cmd").addArgument("/C");
			} else {
				cmdLine = new CommandLine("/bin/bash");
			}
			//Apply command
			cmdLine.addArgument(command);
			
			//Params (PipSQawkling)
			//cmdLine.addArgument(this.getName()+"_iter"+queriesExecuted+"_"+queryToExecute.label);
			
			//Define executor
			DefaultExecutor executor = new DefaultExecutor();
			//Apply Timer 
			executor.setWatchdog(watchdog);
			//Apply Working Dir
			if (workingDir != null)
				executor.setWorkingDirectory(workingDir);
			//Execute & capture status
			exitValue = executor.execute(cmdLine);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return exitValue;
	}
	*/
	
	//Load Additional Commands
	private void loadCommands(String cmdsFileName) {
		if (cmdsFileName == null) return;
		File cmdsFile = new File(cmdsFileName);
		if (!cmdsFile.isFile() ||  !cmdsFile.canRead()) return;
		externalScriptRegistry = new HashMap<String, PipSQream>();
		try {
			//Json Factory and Mapper
			JsonFactory factory = new JsonFactory();
			factory.enable(Feature.ALLOW_COMMENTS);
			JsonParser jp = factory.createJsonParser(cmdsFile);
			ObjectMapper mapper = new ObjectMapper(factory);
			//mapper.enable(SerializationFeature.INDENT_OUTPUT);
			
			//Getting Root
			JsonNode root = mapper.readTree(jp);			
			//Iterating through root
			Iterator<Entry<String,JsonNode>> iter = root.fields();
			while (iter.hasNext()) {
				Entry<String, JsonNode> nodeEntry = iter.next();
				externalScriptRegistry.put(
						nodeEntry.getKey(),
						mapper.treeToValue(nodeEntry.getValue(), PipSQream.class)
					);
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
