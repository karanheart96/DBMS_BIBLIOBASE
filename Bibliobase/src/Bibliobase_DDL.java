import java.sql.SQLException;
import java.sql.Statement;

public class Bibliobase_DDL {
  private static Statement s = null;

  /**
   * Creates an instance of the class that has the sql statement variable for irate
   * @param s the sql statement
   */
  public Bibliobase_DDL(Statement s){

    this.s = s;
  }

  /**
   * Create stored functions in irate.
   */
  public void createFunctions() {
    int success = 0;
    try {
      String proc_isIssn = "CREATE FUNCTION isIssn(" +
        " ISSN  int" +
        ") RETURNS BOOLEAN" +
        " PARAMETER  STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME " +
        " 'Bibliobase_storedfunc.isIssn' ";
      s.executeUpdate(proc_isIssn);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isIssn"+e.getMessage());
    }
    try {
      String proc_parseIssn = "CREATE FUNCTION parseIssn(" +
        " s varchar(16)" +
        ") RETURNS INT" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'Bibliobase_storedfunc.parseIssn' ";
      s.executeUpdate(proc_parseIssn);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function parseIssn"+e.getMessage());
    }

    try {
      String proc_issnToString = "CREATE FUNCTION issnToString(" +
        " ISSN  int" +
        ") RETURNS varchar(16)" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'Bibliobase_storedfunc.issnToString'";
      s.executeUpdate(proc_issnToString);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function issnToString"+e.getMessage());
    }
    try {
      String proc_parseOrcid = "CREATE FUNCTION parseOrcid(" +
        " s varchar(20)" +
        ") RETURNS bigint" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'Bibliobase_storedfunc.parseOrcid'";
      s.executeUpdate(proc_parseOrcid);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function parseOrcid"+e.getMessage());
    }
    try {
      String proc_isOrcid = "CREATE FUNCTION isOrcid(" +
        " ORCID bigint" +
        ") RETURNS BOOLEAN" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'Bibliobase_storedfunc.isOrcid'";
      s.executeUpdate(proc_isOrcid);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isOrcid"+e.getMessage());
    }
    try {
      String proc_orcidToString = "CREATE FUNCTION orcidToString(" +
        " ORCID bigint" +
        ") RETURNS varchar(20)" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'Bibliobase_storedfunc.orcidToString'";
      s.executeUpdate(proc_orcidToString);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function orcidToString"+e.getMessage());
    }
    try {
      String proc_isDoi = "CREATE FUNCTION isDoi(" +
        " DOI varchar(64)" +
        ") RETURNS BOOLEAN" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'Bibliobase_storedfunc.isDoi'";
      s.executeUpdate(proc_isDoi);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isDoi"+e.getMessage());
    }


  }

  /**
   * Creates the tables for the irate database.
   * Created tables: customer, movie, attendance,review, and endorsement.
   */
  public void createTables() {
    int success = 0;
    try {
      s.executeUpdate("create table Publisher ("
        + "  Name varchar(32) not null,"
        + "  City varchar(16) not null,"
        + "  primary key (Name)"
        + ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create Publisher table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table Journal ("
        + "  Title varchar(32) not null,"
        + "  ISSN int not null,"
        + "  primary key (ISSN),"
        + "  CONSTRAINT issn_ck check (isIssn(ISSN)))"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create Journal table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table Article("
        + "  Title varchar(32) not null,"
        + "  DOI varchar(64) not null,"
        + "  primary key (DOI),"
        + "  CONSTRAINT doi_ck check (isDoi(DOI)))"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create Article table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table Author("
        + "  FamilyName varchar(16) not null,"
        + "  GivenName varchar(16) not null,"
        + "  ORCID bigint not null,"
        + "  primary key (ORCID),"
        + "  CONSTRAINT orcid_ck CHECK (isOrcid(ORCID)))"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create Author table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table PublishedBy("
        + "  PublisherName varchar(32) not null,"
        + "  JournalISSN int not null,"
        + "  primary key (JournalISSN),"  // JournalISSN completely determines relation
        + "  foreign key (JournalISSN) references Journal (ISSN) on delete cascade,"
        + "  foreign key (PublisherName) references Publisher (Name) on delete cascade"
        + ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create PublishedBy table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table PublishedIn("
        + "  JournalISSN int not null,"
        + "  ArticleDOI varchar(64) not null,"
        + "  primary key (ArticleDOI)," // ArticleDOI completely determines relation
        + "  foreign key (JournalISSN) references Journal (ISSN) on delete cascade,"
        + "  foreign key (ArticleDOI) references Article (DOI) on delete cascade"
        + ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create PublishedIn table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table WrittenBy("
        + "  ArticleDOI varchar(64) not null,"
        + "  AuthorORCID bigint not null,"
        + "  primary key (ArticleDOI, AuthorORCID),"
        + "  foreign key (ArticleDOI) references Article (DOI) on delete cascade,"
        + "  foreign key (AuthorORCID) references Author (ORCID) on delete cascade"
        + ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create WrittenBy table"+e.getMessage());
    }

  }

  public void createTriggers() {
    int success = 0;

    try {
      String Delete_publishby = "create trigger DeletePublishedBy"
        + " after delete on PublishedBy"
        + " for each statement"
        + "   delete from Journal where issn not in"
        + "     (select JournalIssn from PublishedBy)";
      s.executeUpdate(Delete_publishby);
      success++;
    } catch (SQLException e) {
      System.err.printf("Error creating trigger DeletePublishedBy\n");
    }

    try {
      String Delete_publishin = "create trigger DeletePublishedIn"
        + " after delete on PublishedIn"
        + " for each statement"
        + "   delete from Article where doi not in"
        + "     (select ArticleDOI from PublishedIn)";
      s.executeUpdate(Delete_publishin);
      success++;
    } catch (SQLException e) {
      System.err.printf("Error creating trigger DeletePublishedIn\n");
    }

    try {
      String Delete_writeby = "create trigger DeleteWrittenBy"
        + " after delete on WrittenBy"
        + " for each statement"
        + "   delete from Author where Orcid not in"
        + "     (select AuthorOrcid from WrittenBy)";
      s.executeUpdate(Delete_writeby);
      success++;
    } catch (SQLException e) {
      System.err.printf("Error creating trigger DeleteWrittenBy\n");
    }

  }



  /**
   * Drops all tables in irate
   */
  public void dropTables(String dbTables[]){
    // Drops tables if they already exist
    int dropped = 0;
    for(String table : dbTables) {
      try {
        s.execute("drop table " + table);
        dropped++;
      } catch (SQLException e) {
        System.out.println("did not drop " + table);
      }
    }
    if(dropped == dbTables.length){
      System.out.println("Successfully dropped all tables.");
    }
  }

  /**
   * Drops all functions in irate
   */
  public void dropFunctions(String dbFunctions[]){
    // Drop functions if they already exist
    int dropped = 0;
    for(String fn : dbFunctions) {
      try {
        s.execute("drop function " + fn);
        dropped++;
      } catch (SQLException e) {
        System.out.println("did not drop " + fn);
      }
    }
    if(dropped == dbFunctions.length) {
      System.out.println("Successfully dropped all functions.");
    }
  }

  /**
   * Drops all triggers in irate
   */
  public void dropTriggers(String dbTriggers[]){
    //Drops triggers if they already exists.
    for(String tri : dbTriggers) {
      try {
        s.execute("drop trigger " + tri);
        System.out.println("dropped trigger " + tri);
      } catch (SQLException e) {
        System.out.println("did not drop trigger " + tri);
      }
    }
  }

  /**
   * Drops all procedures in irate (currently unused, no procedures)
   */
  public void dropProcedures(String dbProcedures[]) {
    // Drops procedures if they already exist
    for(String proc : dbProcedures) {
      try {
        s.execute("drop procedure " + proc);
        System.out.println("dropped procedure " + proc);
      } catch (SQLException e) {
        System.out.println("did not drop procedure " + proc);
      }
    }
  }
}
