package tool;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class ReadXML implements Serializable{
	private static final String CONFIG = "connection.xml";
	private static final long serialVersionUID = 1L;
	private static Map<String, String> m_Config = new HashMap<String, String>();
	
	/* 定义sql相关信息  */
	private final String m_SqlDataBase = "database";
	private final String m_SqlName = "username";
	private final String m_SqlPass = "password";
	private final String m_DataBaseUrls = "url";
	private final String m_DataBaseDriver = "driver";
	private final String m_ServerIp = "serverIp";
	private final String m_ServerPort = "serverPort";
	private final String m_DatabaseName = "databaseName";
	
	/* 定义静态变量，作为单例对象引用  */
	private static ReadXML instance = null;
	
	/**
	 * @brief  获取ReadXML类的对象引用，构建一个单例
	 * @param  null
	 * @return void
	 * @remark 用户自定义函数
	 */
	public static ReadXML getInstance() {
		if (instance == null) {
			instance = new ReadXML();
		}
		return instance;
	}
	
	
	private ReadXML() {
		/* 初始化filePath值  */
		String strFilePath = RunningPath.getConfigPath() + CONFIG;
		this.parserXml(strFilePath);
	}
	
	/**
	 * @brief  读取配置文件并缓存相关信息
	 * @param  fileName String字符串表示文件路径
	 * @return void
	 * @remark 用户自定义函数
	 */
	private void parserXml(String fileName) {

		File inputXml = new File(fileName);
		SAXReader saxReader = new SAXReader();
		Document document;		
		try {
			document = saxReader.read(inputXml);
			Element employees = document.getRootElement();

			for (Iterator<?> i = employees.elementIterator(); i.hasNext();) {
				Element employee = (Element) i.next();
				String subject = employee.getName();
				
				if ("Common".equals(subject)) {
					Iterator<?> j = employee.elementIterator();
					for (; j.hasNext();) {
						Element node = (Element) j.next();
						
						if(node.getName().trim().equals(m_SqlDataBase)){
							List<Element> els = node.elements();
							for(Element e :els){
								m_Config.put(e.getName(), e.getTextTrim());
//								if(m_DataBaseUrls.equals(e.getName())){
//									System.out.println(m_Config.get(m_DataBaseUrls));
//								}
							}
						}else{
							m_Config.put(node.getName(), node.getTextTrim());
						}
//						System.out.println("name : " + node.getName() + "value : " + node.getText());
					}
				}			
			} 
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	
	public String getUrl(){
		String url = "jdbc:hhdbsql://"+m_Config.get(m_ServerIp)+":"+m_Config.get(m_ServerPort)+"/"+m_Config.get(m_DatabaseName);
		return url;
	}

	public String getDataBase() {
		return m_Config.get(m_SqlDataBase);
	}

	public String getSqlName() {
		return m_Config.get(m_SqlName);
	}

	public String getSqlPass() {
		return m_Config.get(m_SqlPass);
	}
	
	public String getDataBaseDriver()
	{
		return m_Config.get(m_DataBaseDriver);
	}
	
	public void setUrl(String url){
		m_Config.put(m_DataBaseUrls,url);
	}
	

	public static void main(String[] args) throws IOException, ParseException {
		System.out.println(ReadXML.getInstance().getUrl());
		System.out.println("*******************************************");
		System.out.println(ReadXML.getInstance().getSqlName());
		System.out.println("*******************************************");
		System.out.println(ReadXML.getInstance().getSqlPass());
		System.out.println("*******************************************");
		System.out.println(ReadXML.getInstance().getDataBaseDriver());
		System.out.println("*******************************************");
	
	}
	
}
