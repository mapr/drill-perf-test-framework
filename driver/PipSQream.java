
public class PipSQream {
	String command;
	//String[] envp;
	String execDir;
	Long timeout;

	/**
	 * @return the command
	 */
	public String getCommand() {
		return command;
	}
	/**
	 * @param command the command to set
	 */
	public void setCommand(String command) {
		this.command = command;
	}
	/**
	 * @return the execDir
	 */
	public String getExecDir() {
		return execDir;
	}
	/**
	 * @param execDir the execDir to set
	 */
	public void setExecDir(String execDir) {
		this.execDir = execDir;
	}
	/**
	 * @return the timeout
	 */
	public Long getTimeout() {
		return timeout;
	}
	/**
	 * @param timeout the timeout to set
	 */
	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}
}
