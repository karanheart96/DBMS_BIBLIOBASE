import java.sql.Connection;
import java.sql.Statement;

public class Bibliobase_API {
  // holds an instance of the DML class for insert statements
  private Bibliobase_DML dml = null;

  // holds an instance of the DQL class for select statements
  private Bibliobase_DQL dql = null;

  /**
   * Creates an instance of the API class with instances of the DML and DQL classes
   */
  public Bibliobase_API(Bibliobase_DML dml, Bibliobase_DQL dql){
    this.dml = dml;
    this.dql = dql;
  }

  public void insertrecord(Statement s, Connection conn,String pubname,String city,String joutitle,int issn,String title,String doi,String famname,String gnname,Long orcid) {
    dml.insertPublisher(pubname,city);
    dml.insertJournal(joutitle,issn);
    dml.insertArticle(title,doi);
    dml.insertAuthor(famname,gnname,orcid);
    dml.insertPublishedby(pubname,issn);
    dml.insertPublishedIn(issn,doi);
    dml.insertWrittenBy(doi, orcid);
  }

}
