import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Bibliobase_DQL {
  // connection variables
  Connection conn = null;
  Statement s = null;

  // holds results from queries
  private ResultSet rs = null;

  /**
   * Creates an instance of the class with the sql statement for OrderManager database.
   * @param s the sql statement
   */
  public Bibliobase_DQL(Connection conn, Statement s) {
    this.conn = conn;
    this.s = s;
  }

  public void selectAllPublisher() {
    try {
      ResultSet rs = s.executeQuery("select * from Publisher");
      while(rs.next()) {
        System.out.println(rs.getString("Name"));
        System.out.println(rs.getString("City"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectAllJournal() {
    try{
      ResultSet rs = s.executeQuery("select * from Journal");
      while (rs.next()) {
        System.out.println(rs.getString("Title"));
        System.out.println(rs.getInt("ISSN"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectAllArticle() {
    try {
      ResultSet rs = s.executeQuery("select * from Article");
      while (rs.next()) {
        System.out.println(rs.getString("Title"));
        System.out.println(rs.getString("DOI"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectAllAuthor() {
    try {
      ResultSet rs = s.executeQuery("select * from Author");
      while (rs.next()) {
        System.out.println(rs.getString("FamilyName"));
        System.out.println(rs.getString("GivenName"));
        System.out.println(rs.getLong("ORCID"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectPublishedBy() {
    try {
      ResultSet rs = s.executeQuery("select * from PublishedBy");
      while (rs.next()) {
        System.out.println(rs.getString("PublisherName"));
        System.out.println(rs.getInt("JournalISSN"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectPublishedIn() {
    try {
      ResultSet rs = s.executeQuery("select * from PublishedIn");
      while (rs.next()) {
        System.out.println(rs.getInt("JournalISSN"));
        System.out.println(rs.getString("ArticleDOI"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectWrittenBy() {
    try {
      ResultSet rs = s.executeQuery("select * from WrittenBy");
      while (rs.next()) {
        System.out.println(rs.getString("ArticleDOI"));
        System.out.println(rs.getInt("AuthorORCID"));
      }

    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectPublisherName(String name) {
    try {
      ResultSet rs = s.executeQuery("select * from Publisher where Name = '" + name + "'");
      while(rs.next()) {
        System.out.println(rs.getString("Name"));
        System.out.println(rs.getString("City"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectJoournalISSN(int issn) {
    try {
      ResultSet rs = s.executeQuery("select * from Journal where ISSN = " + issn);
      while (rs.next()) {
        System.out.println(rs.getString("Title"));
        System.out.println(rs.getInt("ISSN"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectArticleDOI(String doi) {
    try {
      ResultSet rs = s.executeQuery("select * from Article where DOI = '" + doi + "'");
      while (rs.next()) {
        System.out.println(rs.getString("Title"));
        System.out.println(rs.getString("DOI"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  public void selectAuthorORCID(String orcid) {
    try {
      ResultSet rs = s.executeQuery("select * from Author where ORCID = '" + orcid + "'");
      while (rs.next()) {
        System.out.println(rs.getString("FamilyName"));
        System.out.println(rs.getString("GivenName"));
        System.out.println(rs.getLong("ORCID"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }
}
