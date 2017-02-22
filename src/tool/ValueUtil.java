package tool;

public class ValueUtil implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	public static int MAX_LEN = 1020;
	public static long parseLong(String input)
	{		
		long value = 0;
		if (!isEmpty(input))
		{
			value = Long.parseLong(input);
		}
		return value;
	}
	
	public static int parseInt(String input)
	{
		int value = 0;
		if (!isEmpty(input))
		{
			value = Integer.parseInt(input);
		}
		return value;
	}
	
	/**
	 * 字符串合法性：是否为空
	 * @param value
	 * @return
	 */
	public static boolean isEmpty(String value) {
		if (value == null || "".equals(value.trim())) {
			return true;
		}
		return false;
	}

	/**
	 * 端口合法性[0,65535]
	 * @param port
	 * @return
	 */
	public static boolean isPort(int port) {
		return (port >= 0 && port <= 65535);
	}
	
	/**
	 * 取大值
	 * @param first
	 * @param second
	 * @return
	 */
	public static long maxLong(long first, long second) {
		return first >= second ? first : second;
	}
	
	/**
	 * 比较大小
	 * @param current
	 * @param compare
	 * @return
	 */
	public static boolean compareLong(long current, long compare) {
		return current >= compare;
	}
}