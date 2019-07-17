import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Bibliobase_DML {
  // database connection variables
  private static Connection conn = null;
  private static Statement s = null;

  // file names are already provided to the class
  private static String Publisher = "publisher.txt";
  private static String Journal = "journal.txt";
  private static String Article = "article.txt";
  private static String Author = "author.txt";
  private static String PublishedBy = "publishedby.txt";
  private static String PublishedIn = "publishedin.txt";
  private static String WrittenBy = "writtenby.txt";

  // Prepared statements for insertions
  private static String insertIntoPublisher = "insert into Publisher(Name,City) values (?,?)";
  private static String insertIntoJournal = "insert into Journal values(?,?)";
  private static String insertIntoArticle = "insert into Article values(?,?)";
  private static String insertIntoAuthor = "insert into Author values(?,?,?)";
  private static String insertIntoPublishedBy = "insert into PublishedBy values(?,?)";
  private static String insertIntoPublishedIn = "insert into PublishedIn values(?,?)";
  private static String insertIntoWrittenBy = "insert into WrittenBy values(?,?)";


  /**
   * Create the DML class with the specified database connection and statement
   *
   * @param conn the connection to database
   * @param s    the sql statement
   */
  public Bibliobase_DML(Connection conn, Statement s) {
    this.conn = conn;
    this.s = s;
  }

  /**
   * Calls all insert into table functions that insert data from respective files into the database.
   */
  public static void insertAll() {
    int success = 0;
    try {
      insertPublisherFile();
    } catch (SQLException e) {
      System.err.printf("Unable to insert into Publisher\n");
      System.err.println(e.getMessage());
    }

    try {
      insertJournalFile();
    }catch (SQLException e) {
      System.err.printf("Unable to insert into journal\n");
      System.err.println(e.getMessage());
    }

    try {
      insertArticleFile();
    }catch (SQLException e) {
      System.err.printf("Unable to insert into article\n");
      System.err.println(e.getMessage());
    }

    try {
      insertAuthorFile();
    }catch (SQLException e) {
      System.err.printf("Unable to insert into author\n");
      System.err.println(e.getMessage());
    }

    try {
      insertPublishedInFile();
    }catch (SQLException e) {
      System.err.printf("Unable to insert into publishedin\n");
      System.err.println(e.getMessage());
    }

    try {
      insertPublishedByFile();
    }catch (SQLException e) {
      System.err.printf("Unable to insert into publishedby\n");
      System.err.println(e.getMessage());
    }

    try {
      insertWrittenByFile();
    }catch (SQLException e) {
      System.err.printf("Unable to insert into writtenby\n");
      System.err.println(e.getMessage());
    }
  }

  /**
   * Inserts the given information into the publisher table.
   * @param Name
   * @param City
   */
  public void insertPublisher(String Name,String City) {
    try {
      PreparedStatement insertRow_publisher = conn.prepareStatement(insertIntoPublisher,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_publisher.setString(1,Name);
      insertRow_publisher.setString(2,City);
      insertRow_publisher.executeUpdate();

      insertRow_publisher.close();
    }catch (SQLException e) {
      System.err.println("Unable to insert" + Name + " : " +City);
    }
  }

  /**
   * Inserts the given information into the journal table.
   * @param Title
   * @param Issn
   */
  public void insertJournal(String Title,int Issn) {
    try {
      PreparedStatement insertRow_journal = conn.prepareStatement(insertIntoJournal,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_journal.setString(1,Title);
      insertRow_journal.setInt(2,Issn);
      insertRow_journal.executeUpdate();

    }catch (SQLException e) {
      System.err.println("Unable to insert" + Title + " : " + Issn);
    }
  }

  /**
   * Inserts the given information into the article table.
   * @param Title
   * @param doi
   */
  public void insertArticle(String Title,String doi) {
    try {
      PreparedStatement insertRow_journal = conn.prepareStatement(insertIntoArticle,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_journal.setString(1,Title);
      insertRow_journal.setString(2,doi);
      insertRow_journal.executeUpdate();

    }catch (SQLException e) {
      System.err.println("Unable to insert" + Title + " : " + doi);
    }
  }

  /**
   * Inserts the given information into the author table.
   * @param FamilyName
   * @param GivenName
   * @param orcid
   */
  public void insertAuthor(String FamilyName,String GivenName,Long orcid) {
    try {
      PreparedStatement insertRow_author = conn.prepareStatement(insertIntoAuthor,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_author.setString(1,FamilyName);
      insertRow_author.setString(2,GivenName);
      insertRow_author.setLong(3,orcid);
      insertRow_author.executeUpdate();
    }catch (SQLException e) {
      System.err.println("Unable to insert" + FamilyName + GivenName + orcid);
    }
  }

  /**
   * Inserts the given information into the publishedby table.
   * @param Name
   * @param issn
   */
  public void insertPublishedby(String Name,int issn) {
    try {
      PreparedStatement insertRow_publishedby = conn.prepareStatement(insertIntoPublishedBy,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_publishedby.setString(1,Name);
      insertRow_publishedby.setInt(2,issn);
      insertRow_publishedby.executeUpdate();
    }catch (SQLException e ) {
      System.err.println("Unable to insert" + Name + issn);
    }
  }

  /**
   * Inserts the given information into the publishedin table.
   * @param issn
   * @param doi
   */
  public void insertPublishedIn(int issn,String doi) {
    try {
      PreparedStatement insertRow_publishedin = conn.prepareStatement(insertIntoPublishedIn,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_publishedin.setInt(1,issn);
      insertRow_publishedin.setString(2,doi);
      insertRow_publishedin.executeUpdate();
    }catch (SQLException e) {
      System.err.println("Unable to insert" + issn + doi);
    }
  }

  /**
   * Inserts the given information into the writtenby table.
   * @param doi
   * @param orcid
   */
  public void insertWrittenBy(String doi,Long orcid) {
    try {
      PreparedStatement insertRow_writtenby = conn.prepareStatement(insertIntoWrittenBy,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_writtenby.setString(1,doi);
      insertRow_writtenby.setLong(2,orcid);
      insertRow_writtenby.executeUpdate();
    }catch (SQLException e) {
      System.err.println("Unable to insert" + doi + orcid);
    }
  }

  /**
   * Inserts information into Publisher table from a txt file
   * @throws SQLException
   */

  public static void insertPublisherFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoPublisher);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(Publisher)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String Name = data[0];
        String City = data[1];

        insertRow_customer.setString(1,Name);
        insertRow_customer.setString(2,City);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert Publisher table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", Publisher);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }


  /**
   * Inserts information into Journal table from a txt file
   * @throws SQLException
   */
  public static void insertJournalFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoJournal);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(Journal)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String Title = data[0];
        String ISSN = data[1];
        int issn = Integer.parseInt(ISSN);

        insertRow_customer.setString(1,Title);
        insertRow_customer.setInt(2,issn);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert Journal table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", Journal);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }


  /**
   * Inserts information into article table from a txt file.
   * @throws SQLException
   */
  public static void insertArticleFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoArticle);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(Article)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String Title = data[0];
        String DOI = data[1];

        insertRow_customer.setString(1,Title);
        insertRow_customer.setString(2,DOI);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert Article table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", Article);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }


  /**
   * Inserts information into author file from a txt file
   * @throws SQLException
   */
  public static void insertAuthorFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoAuthor);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(Author)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String FamilyName = data[0];
        String GivenName = data[1];
        String orcid = data[2];
        long or = Long.parseLong(orcid);


        insertRow_customer.setString(1,FamilyName);
        insertRow_customer.setString(2,GivenName);
        insertRow_customer.setLong(3,or);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert Author table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", Author);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }

  /**
   * Inserts information into publishedby file from a txt file
   * @throws SQLException
   */
  public static void insertPublishedByFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoPublishedBy);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(PublishedBy)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String PublisherName = data[0];
        String Jissn = data[1];
        int jour_issn = Integer.parseInt(Jissn);


        insertRow_customer.setString(1,PublisherName);
        insertRow_customer.setInt(2,jour_issn);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert PublishedBy table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", PublishedBy);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }

  /**
   * Inserts information into publishedin file from a txt file.
   * @throws SQLException
   */
  public static void insertPublishedInFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoPublishedIn);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(PublishedIn)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String ISSN = data[0];
        int is = Integer.parseInt(ISSN);
        String DOI = data[1];

        insertRow_customer.setInt(1,is);
        insertRow_customer.setString(2,DOI);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert PublishedIn table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", PublishedIn);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }

  /**
   * Inserts information into writtenby file from a txt file.
   * @throws SQLException
   */
  public static void insertWrittenByFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoWrittenBy);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(WrittenBy)));
    ) {

      // begin insert line by line
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");

        String DOI = data[0];
        String Orcid = data[1];
        Long or = Long.parseLong(Orcid);

        insertRow_customer.setString(1,DOI);
        insertRow_customer.setLong(2,or);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert WrittenBy table");
        }

      } // end while
    } catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", WrittenBy);
    } catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }

  /**
   * Truncate tables, clearing them of existing data
   *
   * @param dbTables an array of table names
   */
  public static void truncateTables(String dbTables[]) {
    int deleted = 0;
    for(String table : dbTables) {
      try {
        s.executeUpdate("delete from " + table);
        deleted++;
      }
      catch (SQLException e) {
        System.out.println("Did not truncate table " + table);
      }
    }
    if(deleted == dbTables.length){
      System.out.println("Successfully truncated all tables.");
    }
  }


}
