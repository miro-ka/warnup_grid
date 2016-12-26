package org.warnup.grid.tools;

import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteOpenMode;
import java.sql.*;


public class DBWriter {

    private Connection connect() {
        String dbURL = "jdbc:sqlite:/vagrant/tweets/db/tweets.db";
        SQLiteConfig config = new SQLiteConfig();
        config.resetOpenMode(SQLiteOpenMode.CREATE); // this disable creation
        Connection conn = null;

        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection(dbURL, config.toProperties());
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        return conn;
    }

    public void insert(long tweet_id, int count) {
        String sql = "INSERT INTO tweets VALUES ($next_id, ?,?,?,?)";
        long epoch_time = System.currentTimeMillis()/1000;
        int type = 1; //so far default for news

        try{
            Connection conn = this.connect();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(2, epoch_time);
            pstmt.setLong(3, tweet_id);
            pstmt.setInt(4, count);
            pstmt.setInt(5, type);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println("got the freaking errror");
            System.out.println(e.getMessage());
        }
    }
}
