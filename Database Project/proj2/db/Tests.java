package db;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/** Demos the version of assertEquals with a String message. */
public class Tests {
    @Test
    public void joinTest() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("load t2");
        db.transact("load t4");
        Table t1 = db.getTable("t1");
        Table t2 = db.getTable("t2");
        Table t4 = db.getTable("t4");

        Table t3 = Table.joinTables(t1, t2);
        Table t5 = Table.joinTables(t3, t4);
        t3.printTable();
    }

    @Test
    public void rowTest() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("load t2");
        Table t1 = db.getTable("t1");
        Table t2 = db.getTable("t2");
        Row r1 = t1.rowView.get(0);
        Row r2 = t2.rowView.get(0);
        System.out.println(r1);
        System.out.println(r2);
        List<String> comCols = Header.commonCols(r1.headers, r2.headers);
        boolean b = Row.willJoin(comCols, r1, r2);
        System.out.println(b);
        System.out.println(Row.natJoin(comCols, r1, r2));
    }

    @Test
    public void joinSelect() {
        Database db = new Database();
        db.transact("load a");
        db.transact("load b");
//        Table a = db.getTable("a");
//        Table b = db.getTable("b");
//        db.transact("select a from a, b");
    }

    @Test
    public void insertRow() {
        Database db = new Database();
        db.transact("load t1");
        Table t1 = db.getTable("t1");
        String expr = "1, 5";
        Row r = new Row(expr, t1.headers);
        System.out.print(r);
    }

    @Test
    public void addProduct() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("select x + x as b from t1");
    }

    @Test
    public void selectAdvanced() {
        Database db = new Database();
        db.transact("load teams1");
        db.transact("load t7");
        db.transact("select * from teams1, t7");
    }

    @Test
    public void selectWhere() {
        Database db = new Database();
        db.transact("load teams1");
        db.transact("load t7");
        db.transact("select x from teams1, t7 where x > 3");
    }

    @Test
    public void detectTypeTest() {
        System.out.println(Element.detectType("1"));
        System.out.println(Element.detectType("hi"));
        System.out.println(Element.detectType("1.1"));
    }

    @Test
    public void detectTypes() {
        Database db = new Database();
        db.transact("create table t99 (x int, b string)");
        db.transact("insert into t99 values 4, 'b'");
    }

    @Test
    public void typeCheck() {
        Database db = new Database();
        db.transact("load t1");
    }

    @Test
    public void selectFinal() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("select x + y as b from t1");
        db.transact("select x+y as b from t1");
        db.transact("create table t6 as select x + y as b from t1");
        db.transact("create table t6 as select x+y as b from t1");
    }

    @Test
    public void selectMultiple() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("select x + y as b, y from t1");
    }

    @Test
    public void addFinal() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("select x+y as t from t1");
    }

    @Test
    public void stringCompare() {
        Database db = new Database();
        db.transact("load teams");
        db.transact("select TeamName from teams where TeamName > 'g'");
    }

    @Test
    public void selectExpressionsNaN() {
        Database db = new Database();
        db.transact("load selectNaN");
        db.transact("create table t8 as select x/y as b from selectNaN");
    }

    @Test
    public void createselectExpressionsNaN() {
        Database db = new Database();
        db.transact("load selectNaN");
        db.transact("create table t999 as select x/y as b, x from selectNaN");
    }

    @Test
    public void stringConcatenation() {
        Database db = new Database();
        db.transact("load loadBasic2");
        db.transact("select a+b as c from loadBasic2");
    }

    @Test
    public void stringComparison() {
        Database db = new Database();
        db.transact("load alphabet");
        db.transact("select Letter,CodeWord from alphabet where CodeWord < 'India' and Letter >= 'A'");
    }

    @Test
    public void multipleWhere() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("select x from t1 where x > 5 and y > 5");
    }

    @Test
    public void advancedSelect() {
        Database db = new Database();
        db.transact("load teams");
        db.transact("create table complicated as select TeamName, City+Sport as b from teams");
        db.transact("load complicated");
    }

    @Test
    public void sadBasicSelect() {
        Database db = new Database();
        db.transact("load records");
        db.transact("select TeamName,Season,Wins,Losses from records where Wins >= Losses and TeamName > 'Mets'");
    }

    @Test
    public void createBasic() {
        Database db = new Database();
        db.transact("load t1");
        db.transact("create table t6 as select x from t1");
    }

    /** This main method is optional. */
    public static void main(String[] args) {
        jh61b.junit.TestRunner.runTests(Tests.class);
    }
}

