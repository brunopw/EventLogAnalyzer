package eventLogAnalyzer;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EventLogAnalyzer {
	
	private Connection conn = null;
	private String db = "jdbc:hsqldb:hsql://localhost/edb;ifexists=true";
	private String user = "SA";
	private String password = "";
	private List<Event> sortedEventsList = null;
	
	private Connection getConnection() {
    	try {
    		Connection conn = DriverManager.getConnection(db, user, password);
    		conn.setAutoCommit(true);
			return conn;
		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}
    	return null;
	}
	
	private void closeConnection(Connection conn) {
		try {
            if (conn != null) 
                conn.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
	}
	
	private void insertOrUpdateEvent(Event event) {
        try {
        	Connection conn = getConnection();
    		
        	synchronized (EventLogAnalyzer.class) {
	    		if (selectEventByIdUsingConnection(conn, event.getId())) {
	    			if (event.getState().equals(State.FINISHED) && durationNotUpdatedByIdUsingConnection(conn, event.getId()) ) {
		    			List<Event> filteredEvents = sortedEventsList.stream()
		                .filter(ev -> ev.getId().equals(event.getId()))
		                .collect(Collectors.toList());
		    			
		    			// Remove the duplicates events
		    			HashSet<Object> seen = new HashSet<>();
		    			filteredEvents.removeIf(e -> ! seen.add(e.getId() + e.getState().name()));
		    			
		    			long duration = 0;
		    			if(filteredEvents.size() > 1) {
		    				duration = Math.abs(filteredEvents.get(0).getTimestamp().getTime() - filteredEvents.get(1).getTimestamp().getTime());
		    			}
		    			
		    			String sql = "UPDATE Event SET duration = ?, " +
		    										  "alert = ?     " +
							    	 " WHERE id = ?";
						
						PreparedStatement pstmt = conn.prepareStatement(sql);
						pstmt.setTimestamp(1, new Timestamp(duration));
						pstmt.setBoolean(2, duration > 4 ? true : false);
						pstmt.setString(3, event.getId());
		
						pstmt.executeUpdate();
			
						pstmt.close();	   
	    			}
	    		} else {
		        	String sql = "INSERT INTO Event (id, type, host, alert) " +
						    " VALUES (?,?,?,?)";
					
					PreparedStatement pstmt = conn.prepareStatement(sql);
					pstmt.setString(1, event.getId());
					pstmt.setString(2, event.getType());
					pstmt.setString(3, event.getHost());
					pstmt.setBoolean(4, false);
					
					pstmt.executeUpdate();
		
					pstmt.close();	         
		        }
    		}
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
        	closeConnection(conn);
        }
	}
	
	private boolean selectEventByIdUsingConnection(Connection conn, String id) {
		boolean exist = false;
		try {
			String sql = "select * from EVENT where id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, id);
			
            ResultSet rs =  pstmt.executeQuery();
             
            if(rs.next()) {
            	exist = true;
            }
            
            rs.close();
            pstmt.close();
            
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        } 
        return exist;
	}
	
	private boolean durationNotUpdatedByIdUsingConnection(Connection conn, String id) {
		boolean exist = false;
		try {
			String sql = "select * from EVENT where id = ? and duration is null";
            PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, id);
			
            ResultSet rs =  pstmt.executeQuery();
             
            if(rs.next()) {
            	exist = true;
            }
            
            rs.close();
            pstmt.close();
            
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        } 
        return exist;
	}
	
	private void selectEventTable() {
		try {
        	Connection conn = getConnection();

        	Statement stmt = conn.createStatement();
            ResultSet rs =  stmt.executeQuery("select * from EVENT");
             
            System.out.println("----------");
            while(rs.next()) {
                System.out.println("Event: " + rs.getString("ID") + 
                		" " + rs.getTimestamp("DURATION") + 
                		" " + rs.getString("TYPE") + 
                		" " + rs.getString("HOST") +
                		" " + rs.getString("ALERT"));
            }
            System.out.println("----------");
            
            rs.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
        	closeConnection(conn);
        }
	}
	
	private void selectAlertEvents() {
		try {
        	Connection conn = getConnection();

        	Statement stmt = conn.createStatement();
            ResultSet rs =  stmt.executeQuery("select * from EVENT where ALERT = TRUE");
             
            System.out.println("------More than 4ms Events------");
            while(rs.next()) {
                System.out.println(rs.getString("ID") + 
                		" " + rs.getTimestamp("DURATION") + 
                		" " + rs.getString("TYPE") + 
                		" " + rs.getString("HOST") + 
                		" " + rs.getString("ALERT")); 
            }
            System.out.println("--------------------------------");
            
            rs.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
        	closeConnection(conn);
        }
	}
	
	private void dropTable() {
		try {
        	Connection conn = getConnection();

        	Statement stmt = conn.createStatement();
	        stmt.execute("DROP TABLE event");
	        
	        stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
            closeConnection(conn);
        }
	}
	
	private void deleteTable() {
		try {
        	Connection conn = getConnection();

        	Statement stmt = conn.createStatement();
	        stmt.execute("DELETE FROM event");
	        
	        stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
            closeConnection(conn);
        }
	}
	
	private void createTable() {
		//deleteTable();
		//selectEventTable();
		dropTable();
        try {
        	Connection conn = getConnection();

        	Statement stmt = conn.createStatement();
	        stmt.execute("CREATE TABLE IF NOT EXISTS event ( " + 
						"id VARCHAR(255)," + 
						"duration TIMESTAMP," + 
						"type VARCHAR(100)," + 
						"host VARCHAR(255)," + 
						"alert BOOLEAN" + 
						" )");
	        
	        stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
            closeConnection(conn);
        }
	}

	public static void main(String[] args)
			throws JsonGenerationException, JsonMappingException, IOException, SQLException, 
			ClassNotFoundException {
		
		EventLogAnalyzer ela = new EventLogAnalyzer();
		ela.populateSortedEventsList();
		ela.sortedEventsList.forEach(e -> System.out.println(e.getId() + " - " + e.getState()));
		
		ela.createTable();
        
		for (int i = 0; i < ela.sortedEventsList.size(); i++) {
			ela.insertOrUpdateEvent(ela.sortedEventsList.get(i));
		}
		//ela.selectEventTable();
		ela.selectAlertEvents();
	}

	private void populateSortedEventsList() throws IOException {
		ObjectMapper mapper = new ObjectMapper();

		MappingIterator<Event> eventMap = mapper.readerFor(Event.class).readValues(new File("logfile.txt"));

		this.sortedEventsList = eventMap.readAll().stream().sorted(Comparator.comparing(Event::getState))
				.sorted(Comparator.comparing(Event::getId)).collect(Collectors.toList());

		eventMap.readAll().stream().sorted(Comparator.comparing(Event::getState))
				.sorted(Comparator.comparing(Event::getId)).collect(Collectors.toList());
	}
}
