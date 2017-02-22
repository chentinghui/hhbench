package tool;


/** 
* @ClassName: RunningPath
* @Description:获取程序bin目录路径，config目录路径，root目录路径
* @author lvcx
* @date 2014/5/1
*/
public class RunningPath implements java.io.Serializable{
	private static final long serialVersionUID = 4554341785798185128L;

	private static final String CONFIG = "conf/";

	private static final String SPACE = "%20";
	
	
	/**
	 * @brief  获取bin目录路径
	 * @param  null
	 * @return void
	 * @remark 用户自定义函数
	 */
	public static String getBinPath() {
//		System.out.println(Class.class.getClass().getResource("/")+"################");
		String currentPath = Class.class.getClass().getResource("/").getPath();
		currentPath = currentPath.substring(0, currentPath.length());
		return replaceSpace(currentPath);
	}

	/**
	 * @brief  获取配置文件的路径
	 * @param  null
	 * @return void
	 * @remark 用户自定义函数
	 */
	public static String getConfigPath() {
		String currentBinPath = getBinPath();
		return currentBinPath + "../" + CONFIG;
	}

	/**
	 * @brief  获取root目录路径
	 * @param  null
	 * @return void
	 * @remark 用户自定义函数
	 */
	public static String getRootPath() {
		String currentBinPath = getBinPath();
		return currentBinPath + "../";
	}
	
	private static String replaceSpace(String value) {
		if (!ValueUtil.isEmpty(value)) {
			value = value.replaceAll(SPACE, " ");
		}
		return value;
	}
}
