package main;

import java.beans.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.PropertyConfigurator;
import org.hhdbsql.copy.CopyManager;
import org.hhdbsql.core.BaseConnection;

import tool.ReadXML;




public class test extends Thread{
	static Connection conn= null;
	static PreparedStatement  pstmt = null;
	public static void main(String[] args) throws Exception {
	
		  String s1 = "Programming";
	        String s2 = new String("Programming");
	        String s3 = "Program";
	        String s4 = "ming";
	        String s5 = "Program" + "ming";
	        String s6 = s3 + s4;
	        System.out.println(s1 == s2);
	        System.out.println(s1 == s5);
	        System.out.println(s1 == s6);
	        System.out.println(s1 == s6.intern());
	        System.out.println(s2 == s2.intern());
	}
	
	public static   Connection  createConnection() throws Exception{
		try {
			Class.forName(ReadXML.getInstance().getDataBaseDriver());
			String url = ReadXML.getInstance().getUrl();
			String name = ReadXML.getInstance().getSqlName();
			String password = ReadXML.getInstance().getSqlPass();
			conn = DriverManager.getConnection(url, name, password);
			
			return conn;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}
	
	// 第一种方法：设定指定任务task在指定时间time执行 schedule(TimerTask task, Date time)  
    public static void timer1() {  
        Timer timer = new Timer();  
        timer.schedule(new TimerTask() {  
            public void run() {  
                System.out.println("-------设定要指定任务--------");  
            }  
        },0);// 设定指定的时间time,此处为2000毫秒  
    }  
	
	private static void initLog4j_debug() {
		Properties prop = new Properties();

		prop.setProperty("log4j.rootLogger", "DEBUG, CONSOLE");
		prop.setProperty("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender ");
		prop.setProperty("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
//		prop.setProperty("log4j.appender.CONSOLE.Append", "true");
//		prop.setProperty("log4j.appender.CONSOLE.File", "notify-subscription."+ManagementFactory.getRuntimeMXBean().getName()+".log ");
		prop.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", " %m  %r %n");
	
//		prop.setProperty("log4j.rootLogger", "DEBUG, CONSOLE");
//		prop.setProperty("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
//		prop.setProperty("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
//		prop.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{HH:mm:ss,SSS} [%t] %-5p %C{1} : %m%n");
		PropertyConfigurator.configure(prop);
	}
	
	private static void initLog4j_log() {
		Properties prop = new Properties();

		prop.setProperty("log4j.rootLogger", "DEBUG, l");
		prop.setProperty("log4j.appender.l", "org.apache.log4j.DailyRollingFileAppender ");
		prop.setProperty("log4j.appender.l.layout", "org.apache.log4j.PatternLayout");
		prop.setProperty("log4j.appender.l.Append", "false");
		prop.setProperty("log4j.appender.l.File", "notify-subscription.log ");
		prop.setProperty("log4j.appender.l.layout.ConversionPattern", "%d ssdfs [%t] %-5p %C{1} : %m%n");
	
		
		PropertyConfigurator.configure(prop);
	}
	
	static void testdemo2(){
		List<Integer> scripts_random = new ArrayList<Integer>();
		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < 3  ; j++) {
				scripts_random.add(i);
			}
		}
		
		System.out.println(scripts_random);
//		for (int i = 0; i < 50; i++) {
//			java.util.Random random =new java.util.Random();
//			int result_num_scripts=scripts_random.get(random.nextInt(scripts_random.size()));
//			System.out.println(result_num_scripts);
//		}
		
	}
	
	
	
		
	static void testDome(){
		//读取文件代码
		try {
			//1、创建流对象
	        Reader reader=new FileReader("ReadMe.txt");
	        //构建高效流对象
	        BufferedReader buffReader=new BufferedReader(reader);
	         
	        //2、读取一行字符串
	        String line=buffReader.readLine();
	        StringBuffer buffer=new StringBuffer();
	        while(line!=null){
	            buffer.append(line+"\r\n");
	            line=buffReader.readLine();
	        }
	        System.out.println(buffer.toString());;
	        //3、关闭流
	        buffReader.close();
	        reader.close();
		}  catch (FileNotFoundException e) {
            System.out.println("要读取的文件不存在："+e.getMessage());
        } catch (IOException e) {
            System.out.println("文件读取错误："+e.getMessage());
        }
	
	}
}

