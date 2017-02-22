package main;

public enum QueryMode{
	QUERY_SIMPLE("simple"),				/* simple query */
	QUERY_EXTENDED("extended"),				/* extended query */
	QUERY_PREPARED("prepared");				/* extended query with prepared statements */
	
	String name;
	private QueryMode(String name){
		this.name = name;
	}
	
}
