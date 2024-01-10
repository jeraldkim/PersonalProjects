package db;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Database {

    // Various common constructs, simplifies parsing.
    private static final String REST  = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND   = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD   = Pattern.compile("load " + REST),
            STORE_CMD  = Pattern.compile("store " + REST),
            DROP_CMD   = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD  = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*"
            + "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+"
                    + "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+"
                    + "([\\w\\s+\\-*/'<>=!]+?(?:\\s+and\\s+"
                    + "[\\w\\s+\\-*/'<>=!]+?)*))?"),
            CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+"
                    + SELECT_CLS.pattern()),
            INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?"
                    + "\\s*(?:,\\s*.+?\\s*)*)");

    private Map<String, Table> tables;

    public Database() {
        tables = new HashMap<>();
    }

    public String transact(String query) {
        return eval(query);
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public Table[] getTables(String[] tableNames) {
        Table[] returnTables = new Table[tableNames.length];
        for (int i = 0; i < tableNames.length; i += 1) {
            returnTables[i] = getTable(tableNames[i]);
        }
        return returnTables;
    }

    public String eval(String query) {
        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return storeTable(m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return dropTable(m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insertRow(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1));
        } else {
            String msg = "ERROR: *.";
            return msg;
        }
    }

    private String createTable(String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(COMMA));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
        } else {
            return "ERROR: *.";
        }
    }

    private String createNewTable(String name, String[] l) {
        Table newTable = new Table(l.length);

        for (int i = 0; i < l.length; i++) {
            String str = l[i];
            String[] splitted = str.split("\\s+");
            Boolean a = splitted[1].equals("string");
            Boolean b = splitted[1].equals("int");
            Boolean c = splitted[1].equals("float");
            if (a || b || c) {
                newTable.headers.setIVal(i, splitted[0]);
                newTable.headers.setIType(i, splitted[1]);
            } else {
                return "ERROR: *.";
            }
        }
        tables.put(name, newTable);
        return "";
    }

    private String createSelectedTable(String name, String exprs, String tables1, String conds) {
        String[] colNames = exprs.split(COMMA);
        String[] tableNames = tables1.split(COMMA);
        Table[] tablelst = getTables(tableNames);
        List<Table> list = Arrays.asList(tablelst); //checks if null is in the list of tables
        if (list.contains(null)) { //contains empty table
            return "ERROR: *.";
        }
        Table joined = Table.joinTables(tablelst); //COMBINED TABLE
        Table[] tbls = new Table[colNames.length]; //list of tables
        if (colNames.length > 1) {
            for (int i = 0; i < colNames.length; i++) {
                String col = colNames[i];
                //tests for operators
                Boolean a = col.contains("+");
                Boolean b = col.contains("-");
                Boolean c = col.contains("/");
                Boolean d = (col.contains("*") && col.length() != 1);
                if (a || b || c || d) {
                    String[] colsplit = col.split("\\s+");
                    Table newtbl = selectOperatorMultiple(colsplit, joined);
                    tbls[i] = newtbl;
                } else {
                    String[] arg = new String[]{col};
                    Table newtbl2 = joined.project(arg);
                    tbls[i] = newtbl2;
                }
            }
            joined = Table.appendTables(tbls);
        }
        Table operated;
        if (colNames.length == 1) {
            for (int i = 0; i < colNames.length; i++) {
                Boolean a = colNames[i].contains("+");
                Boolean b = colNames[i].contains("-");
                Boolean c = colNames[i].contains("/");
                Boolean d = (colNames[i].contains("*")) && (colNames.length != 1);
                if (a || b || c || d) {
                    String[] words = colNames[0].split("\\s+");
                    operated = selectOperator2(words, tableNames);
                    this.tables.put(name, operated);
                    return "";
                }
            }
        }
        if (conds != null) {
            String[] condslst = conds.split(AND);
            for (int i = 0; i < condslst.length; i++) {
                String colnames1 = condslst[i].split("\\s+")[0];
                String comparator = condslst[i].split("\\s+")[1];
                String secondVal = condslst[i].split("\\s+")[2];
                Boolean a = Element.detectType(secondVal).equals("int");
                Boolean b = Element.detectType(secondVal).equals("float");
                Boolean c = secondVal.startsWith("\'");
                if (a || b || c) {
                    joined = joined.unaryWhere(colnames1, secondVal, comparator);
                } else {
                    joined = joined.binaryWhere(colnames1, secondVal, comparator);
                }
            }
        }
        Boolean cont = false;
        int single = 0;
        for (int i = 0; i < colNames.length; i++) {
            Boolean confused = colNames[i].contains(" ");
            if (confused) {
                break;
            }
            single++;
        }
        if (single == colNames.length) {
            cont = true;
        }
        Table rv = joined;
        if (cont) {
            rv = joined.project(colNames);
        }
        this.tables.put(name, rv);
        return "";
    }

    private String loadTable(String name) {
        System.out.printf("You are trying to load the table named %s\n", name);
        String fileName = name + ".tbl";

        try {
            File file = new File(fileName);
            boolean exists = file.exists();
            if (!exists) {
                return "ERROR: *.";
            }
            Table newTable = new Table(fileName);
            tables.put(name, newTable);
            System.out.println(newTable);

        } catch (ArrayIndexOutOfBoundsException e) {
            return "ERROR: *.";
        } catch (NullPointerException e) {
            return "ERROR: *.";
        } catch (RuntimeException e) {
            return "ERROR: runtime";
        }
        return "";
    }

    private String storeTable(String name) {
        try {
            Table curr = tables.get(name);
            Header currh = curr.headers;
            List<Row> currr = curr.rowView;

            String finalName = name + ".tbl";

            File file = new File(finalName);
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));

            for (int i = 0; i < currh.numCols - 1; i += 1) { //Writes headers for thecolumns first
                String a = currh.getIVal(i) + " ";
                String b = currh.getIType(i) + ",";
                writer.write(a + b); //must remove white space for the end
            }
            String c = currh.getIVal(currh.numCols - 1);
            String d = currh.getIType(currh.numCols - 1) + "\n";
            writer.write(c + " " + d);

            // Now write down the actual contents of the table

            for (int x = 0; x < currr.size(); x++) {
                Row r = currr.get(x);
                for (int y = 0; y < r.size - 1; y++) {
                    writer.write(r.data.get(r.headers.getIVal(y)).val + ",");
                }
                writer.write(r.data.get(r.headers.getIVal(r.size - 1)).val + "\n");
            }
            writer.flush();
            writer.close();

        } catch (IOException e) {
            return "ERROR: *.";
        } catch (NullPointerException e) {
            return "ERROR: haha i caught you.";
        } catch (ArrayIndexOutOfBoundsException e) {
            return "ERROR: haha i caught you.";
        } catch (IndexOutOfBoundsException e) {
            return "ERROR: :p";
        }
        return "";
    }

    private String dropTable(String name) {
        if (!tables.containsKey(name)) {
            return "ERROR: *.";
        }
        tables.remove(name);
        return "";
    }

    private String printTable(String name) {
        if (!this.tables.containsKey(name)) {
            return "ERROR: *.";
        }
        return this.tables.get(name).toString();
    }

    private String select(String expr) {
        try {
            Matcher m = SELECT_CLS.matcher(expr);
            if (!m.matches()) {
                return "ERROR: *.";
            }
            String[] colNames = m.group(1).split(COMMA); //column expressions
            String[] tableNames = m.group(2).split(COMMA);
            Table[] tables1 = getTables(tableNames);
            Table rv = Table.joinTables(tables1);
            if (expr.contains("where")) {
                String[] conds = m.group(3).split(AND);
                for (int i = 0; i < conds.length; i++) {
                    String colnames1 = conds[i].split("\\s+")[0];
                    String comparator = conds[i].split("\\s+")[1];
                    String secondVal = conds[i].split("\\s+")[2];
                    int select2run = rv.headers.header.length;
                    int select2call = 0;
                    for (int j = 0; j < select2run; j++) {
                        if (rv.headers.header[j][0] == colnames1) {
                            break;
                        }
                        select2call++;
                    }
                    if (select2call == select2run) {
                        return select2(expr);
                    }
                    Boolean denero = Element.detectType(secondVal).equals("int");
                    Boolean hug = Element.detectType(secondVal).equals("float");
                    Boolean hilfinger = secondVal.startsWith("\'");
                    if (denero || hug || hilfinger) {
                        rv = rv.unaryWhere(colnames1, secondVal, comparator);
                    } else {
                        rv = rv.binaryWhere(colnames1, secondVal, comparator);
                    }
                }
            }
            Table[] tbls = new Table[colNames.length];
            if (colNames.length > 1) {
                for (int i = 0; i < colNames.length; i++) {
                    String col = colNames[i];
                    Boolean korea = col.contains("+") || col.contains("-");
                    Boolean china = col.contains("/");
                    Boolean usa = (col.contains("*") && col.length() != 1);
                    if (korea || china || usa) {
                        String[] colsplit = col.split("\\s+");
                        tbls[i] = selectOperatorMultiple(colsplit, rv);
                    } else {
                        tbls[i] = rv.project(new String[]{col});
                    }
                }
                return Table.appendTables(tbls).toString();
            }
            if (colNames.length == 1) {
                String[] group1 = m.group(1).split("\\s+");
                Table operated;
                for (int i = 0; i < group1.length; i++) {
                    Boolean cal = group1[i].contains("+");
                    Boolean stanford = group1[i].contains("-");
                    Boolean ucla = group1[i].contains("/");
                    Boolean sfsu = (group1[i].contains("*")) && (group1.length != 1);
                    if (cal || stanford || ucla || sfsu) {
                        return selectOperator(group1, tableNames);
                    }
                }
            }
            Table single = rv.project(colNames);
            return single.toString();
        } catch (NullPointerException e) {
            return "ERROR: haha i caught you.";
        } catch (ArrayIndexOutOfBoundsException e) {
            return "ERROR: haha i caught you.";
        } catch (IndexOutOfBoundsException e) {
            return "ERROR: :p";
        } catch (RuntimeException e) {
            return "ERROR: better luck next time";
        }
    }

    private String select2(String expr) {
        try {
            Matcher m = SELECT_CLS.matcher(expr);
            if (!m.matches()) {
                return "ERROR: *.";
            }
            String[] colNames = m.group(1).split(COMMA); //column expressions
            String[] tableNames = m.group(2).split(COMMA);
            Table[] tables1 = getTables(tableNames);
            Table rv = Table.joinTables(tables1);
            Table[] tbls = new Table[colNames.length + 1];
            tbls[0] = rv;
            Boolean ran = false;
            int startingindex = 1;
            if (colNames.length > 1) {
                for (int i = 0; i < colNames.length; i++) {
                    String col = colNames[i];
                    Boolean nihao = col.contains("+") || col.contains("-");
                    Boolean anyoung = col.contains("/");
                    Boolean hola = (col.contains("*") && col.length() != 1);
                    if (nihao || anyoung || hola) {
                        String[] colsplit = col.split("\\s+");
                        Table newtbl = selectOperatorMultiple(colsplit, rv);
                        tbls[startingindex] = newtbl;
                        startingindex += 1;
                        ran = true;
                    }
                }
                rv = Table.appendTables(tbls);
            }
            if (expr.contains("where")) {
                String[] conds = m.group(3).split(AND);
                for (int i = 0; i < conds.length; i++) {
                    String colnames1 = conds[i].split("\\s+")[0];
                    String comparator = conds[i].split("\\s+")[1];
                    String secondVal = conds[i].split("\\s+")[2];
                    Boolean alex = Element.detectType(secondVal).equals("int");
                    Boolean jerry = Element.detectType(secondVal).equals("float");
                    Boolean andy = secondVal.startsWith("\'");
                    if (alex || jerry || andy) {
                        rv = rv.unaryWhere(colnames1, secondVal, comparator);
                    } else {
                        rv = rv.binaryWhere(colnames1, secondVal, comparator);
                    }
                }
            }
            if (colNames.length == 1) {
                String[] group1 = m.group(1).split("\\s+");
                for (int i = 0; i < group1.length; i++) {
                    Boolean lol = group1[i].contains("+") || group1[i].contains("-");
                    Boolean over = group1[i].contains("/");
                    Boolean mine = (group1[i].contains("*")) && (group1.length != 1);
                    if (lol || over || mine) {
                        return selectOperator(group1, tableNames);
                    }
                }
            }
            if (!ran) {
                return rv.project(colNames).toString();
            } else {
                Table multiple = rv;
                String[] chosenOnes = new String[colNames.length];
                for (int i = 0; i < chosenOnes.length; i++) {
                    if (colNames[i].length() > 1) {
                        String[] chosenBorn = colNames[i].split("\\s+");
                        chosenOnes[i] = chosenBorn[chosenBorn.length - 1];
                    } else {
                        chosenOnes[i] = colNames[i];
                    }
                }
                return multiple.project(chosenOnes).toString();
            }
        } catch (NullPointerException e) {
            return "ERROR: haha i caught you.";
        } catch (ArrayIndexOutOfBoundsException e) {
            return "ERROR: haha i caught you.";
        } catch (IndexOutOfBoundsException e) {
            return "ERROR: :p";
        }
    }

    public String selectOperator(String[] group1, String[] tableNames) {
        return selectOperator2(group1, tableNames).toString();
    }

    public Table selectOperator2(String[] group1, String[] tableNames) {
        //for case where the spaces are NOT made create the if statement
        String colName1;
        String colName2;
        String operation;
        String newName;
        if (group1.length == 3) { //might have case such as 7+ 9
            String first = group1[0];
            newName = group1[2];
            String[] expr;
            if (group1[0].contains("+")) {
                expr = group1[0].split("\\+");
                operation = "+";
            } else if (group1[0].contains("-")) {
                expr = group1[0].split("\\-");
                operation = "-";
            } else if (group1[0].contains("/")) {
                expr = group1[0].split("\\/");
                operation = "/";
            } else {
                expr = group1[0].split("\\*");
                operation = "*";
            }
            colName1 = expr[0];
            colName2 = expr[1];
        } else {
            colName1 = group1[0];
            colName2 = group1[2];
            operation = group1[1];
            newName = group1[4];
        }
        Table[] tables1 = getTables(tableNames);
        Table joined = Table.joinTables(tables1);
        Table finaltable = joined.godProject(colName1, colName2, newName, operation);
        return finaltable;
    }

    public Table selectOperatorMultiple(String[] group1, Table tbl) {
        //for case where the spaces are NOT made create the if statement
        String colName1;
        String colName2;
        String operation;
        String newName;
        if (group1.length == 3) { //might have case such as 7+ 9
            String first = group1[0];
            newName = group1[2];
            String[] expr;
            if (group1[0].contains("+")) {
                expr = group1[0].split("\\+");
                operation = "+";
            } else if (group1[0].contains("-")) {
                expr = group1[0].split("\\-");
                operation = "-";
            } else if (group1[0].contains("/")) {
                expr = group1[0].split("\\/");
                operation = "/";
            } else {
                expr = group1[0].split("\\*");
                operation = "*";
            }
            colName1 = expr[0];
            colName2 = expr[1];
        } else {
            colName1 = group1[0];
            colName2 = group1[2];
            operation = group1[1];
            newName = group1[4];
        }
        Table finaltable = tbl.godProject(colName1, colName2, newName, operation);
        return finaltable;
    }

    private String select(String[] colNames, String[] tableNames) {

        Table[] tables2 = getTables(tableNames);
        Table joined = Table.joinTables(tables2);
        String rv = joined.project(colNames).toString();
        return rv;
    }

    public String insertRow(String tableName, String[] literals) {
        if (!this.tables.containsKey(tableName)) { //table exists?
            return "ERROR: *.";
        } else {
            Table tbl = tables.get(tableName);
            if (tbl.numCols != literals.length) { //column length == values.length()
                return "ERROR: *.";
            }
            for (int i = 0; i < tbl.headers.numCols; i++) { //type same?
                Boolean maguro = !tbl.headers.header[i][1].equals(Element.detectType(literals[i]));
                Boolean unagi = !literals[i].equals("NaN");
                Boolean hamachi = !literals[i].equals("NOVALUE");
                if (maguro && unagi && hamachi) {
                    return "ERROR: *.";
                }
            }
        }
        Row newRow = new Row(literals, tables.get(tableName).headers);
        tables.get(tableName).rowView.add(newRow);
        return "";
    }

    private String insertRow(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            String rv = "ERROR: *.";
            return rv;
        }
        String[] values = m.group(2).split(COMMA);
        String nameOfTable = m.group(1);

        return this.insertRow(nameOfTable, values);
    }
}
