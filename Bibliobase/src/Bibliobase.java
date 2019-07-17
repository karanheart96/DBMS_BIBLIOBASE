import java.sql.Connection;
import java.sql.Statement;

public class Bibliobase {
  // connection variables
  private static Connection conn = null;
  private static Statement s = null;

  /** Name of the database */
  private static String dbName = "bibliobase";

  /** Contains the names of the tables in irate */
  private static String dbTables[]= {
    "PublishedBy", "PublishedIn", "WrittenBy",		// relations
    "Author", "Article", "Journal", "Publisher"		// entities
  };

  /** Contains names of stored functions in irate */
  private static String dbFunctions[]={
    "isIssn","parseIssn","issnToString","parseOrcid",
    "isOrcid","orcidToString","isDoi"
  };

  /* Contains names of triggers in irate (unused)*/
  private static String dbTriggers[] = {
    "DeleteWrittenBy", "DeletePublishedIn", "DeletePublishedBy"
  };

  /* Contains names of procedures in irate (unused)*/
  private static String dbProcedures[] = {
    // none
  };

  public static void main(String[] args) {
    Bibliobase_Connection bib = new Bibliobase_Connection();
    bib.startConnection("user1","password",dbName);
    conn = bib.getConnection();
    s = bib.getStatement();

    Bibliobase_DDL ddl = new Bibliobase_DDL(s);
    Bibliobase_DML dml = new Bibliobase_DML(conn,s);
    Bibliobase_DQL dql = new Bibliobase_DQL(conn,s);
    Bibliobase_API api = new Bibliobase_API(dml,dql);

    System.out.println("Initializing database...");
    System.out.println("\nDropping tables and functions and triggers:");
    ddl.dropTables(dbTables);
    ddl.dropFunctions(dbFunctions);
    // trigger drop not needed, because dropping tables drops the triggers as well
    // ddl.dropTriggers(s, dbTriggers);


    System.out.println("\nCreating functions:");
    ddl.createFunctions();


    System.out.println("\nCreating tables:");
    ddl.createTables();

    System.out.println("\nCreating triggers:");
    ddl.createTriggers();

    // truncate, then insert data into tables
    System.out.println("\nTruncating tables:");
    dml.truncateTables(dbTables);
    System.out.println("\nInserting values:");
    dml.insertAll();

    api.insertrecord(s,conn,"AWSW","California","Computersystems",1542-8890,"what is a RAM?","10.1145/3134434.3136787","guy","katcher", (long) (0000-0002-4543-4532));



  }
}
