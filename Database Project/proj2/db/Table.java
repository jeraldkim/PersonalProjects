package db;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;


public class Table {
    public int numCols;
    public Header headers; //0th item is name 1st element is type of data
    public List<Row> rowView = new ArrayList<>();

    public Table() {
        this.numCols = 0;
        headers = new Header(numCols);
        rowView = new ArrayList<>();
    }

    public Table(int numCols) {
        this.numCols = numCols;
        headers = new Header(numCols);
        rowView = new ArrayList<>();
    }

    public Table(Table t) {
        numCols = t.numCols;
        headers = new Header(t.headers);

        for (Row r : t.rowView) {
            rowView.add(new Row(r));
        }
    }

    public Table(String fileName) {
        //copied from net. Cite in the end
        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            line = bufferedReader.readLine();
            headers = new Header(line); //creates the header through scanning
            numCols = headers.numCols;

            for(String lineRow; (lineRow = bufferedReader.readLine()) != null; ) {
                String[] splitted = lineRow.split("\\s*,\\s*");
                for (int i = 0; i < splitted.length; i += 1) {
                    if (!Element.detectType(splitted[i]).equals(headers.getIType(i)) && !splitted[i].equals("NOVALUE") && !splitted[i].equals("NaN")) {
                        throw new RuntimeException("type mismatch");
                    }
                }
                rowView.add(new Row(lineRow, headers));
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
            // Or we could just do this: ex.printStackTrace();
        }


    }

    public static Table crossProduct(Table t1, Table t2) {
        Table newTable = new Table(t1.numCols + t2.numCols);
        newTable.headers = Header.joinHeaders(t1.headers, t2.headers);
        for (int i = 0; i < t1.numCols; i += 1) {
            for (int j = 0; j < t2.numCols; j += 1) {
                Row newRow = Row.joinRows(t1.rowView.get(i), t2.rowView.get(j));
                newTable.rowView.add(newRow);
            }
        }

        return newTable;
    }

    public static Table crossProduct(Table[] tables) {
        if (tables.length == 2) {
            return crossProduct(tables[0], tables[1]);
        }
        Table[] restOfTables = Arrays.copyOfRange(tables, 2, tables.length);
        return crossProduct(crossProduct(tables[0], tables[1]), crossProduct(restOfTables));
    }

    public static Table joinTables(Table[] tables) {
        if (tables.length == 1) {
            return tables[0];
        }
        Table[] restOfTables = Arrays.copyOfRange(tables, 1, tables.length);
        return joinTables(tables[0], joinTables(restOfTables));
    }

    public static Table joinTables(Table t1, Table t2) {
        Table finalTable = new Table();
        finalTable.headers = Header.natJoinHeaders(t1.headers, t2.headers);
        finalTable.numCols = finalTable.headers.numCols;

        List<String> commonCols = Header.commonCols(t1.headers, t2.headers);

        for (Row r1 : t1.rowView) {
            for (Row r2 : t2.rowView) {
                if (Row.willJoin(commonCols, r1, r2)) {
                    finalTable.rowView.add(Row.natJoin(commonCols, r1, r2));
                }
            }
        }

        return finalTable;
    }

    public Table project(String[] colNames) {
        if (colNames.length == 1 && colNames[0].equals("*")) {
            return this;
        }
        Table newTable = new Table(colNames.length);
        newTable.headers = new Header(colNames.length);
        for (int i = 0; i < colNames.length; i += 1) {
            newTable.headers.header[i][0] = colNames[i];
            newTable.headers.header[i][1] = this.headers.getType(colNames[i]);
        }

        for (int i = 0; i < rowView.size(); i += 1) {
            Row newRow = rowView.get(i).subRow(colNames);
            newTable.rowView.add(newRow);
        }
        return newTable;
    }

    public Table addProject(String colName1, String colName2, String alias) {
        Table t1 = project(new String[]{colName1, colName2});
        Table returnTable = new Table(1);
        returnTable.headers.setIVal(0, alias);

        String type1 = t1.headers.getType(colName1);
        String type2 = t1.headers.getType(colName2);

        if (type1.equals("string") && type2.equals("string")) {
            returnTable.headers.setIType(0, "string");
        }
        else if (type1.equals("int") && type2.equals("int")) {
            returnTable.headers.setIType(0, "int");
        }
        else if ((type1.equals("int") || type1.equals("float")) && (type2.equals("int") || type2.equals("float"))) {
            returnTable.headers.setIType(0, "float");
        }
        else {
            throw new RuntimeException("Trying to add incompatible columns of type :" + type1 + " and " + type2);
        }
        for (Row r: t1.rowView) {
            Row newRow = new Row();
            newRow.size = 1;
            newRow.headers = returnTable.headers;
            Element firstElement = r.data.get(colName1);
            Element secondElement = r.data.get(colName2);
            Element newElement = firstElement.add(secondElement);
            newRow.data.put(alias, newElement);
            returnTable.rowView.add(newRow);
        }
        return returnTable;
    }

    public Table subMulDivProject(String colName1, String colName2, String alias, String operator) {
        Table t1 = project(new String[]{colName1, colName2});
        Table returnTable = new Table(1);
        returnTable.headers.setIVal(0, alias);

        String type1 = t1.headers.getType(colName1);
        String type2 = t1.headers.getType(colName2);

        if (type1.equals("int") && type2.equals("int")) {
            returnTable.headers.setIType(0, "int");
        }
        else if ((type1.equals("int") || type1.equals("float")) && (type2.equals("int") || type2.equals("float"))) {
            returnTable.headers.setIType(0, "float");
        }
        else {
            throw new RuntimeException("Trying to subtract incompatible columns of type :" + type1 + " and " + type2);
        }
        for (Row r: t1.rowView) {
            Row newRow = new Row();
            newRow.size = 1;
            newRow.headers = returnTable.headers;
            Element firstElement = r.data.get(colName1);
            Element secondElement = r.data.get(colName2);
            Element newElement = firstElement.arithmetic(secondElement, operator);
            newRow.data.put(alias, newElement);
            returnTable.rowView.add(newRow);
        }
        return returnTable;
    }

    public Table godProject(String colName1, String colName2, String alias, String operator) {
        if (operator.equals("+")) {
            return addProject(colName1, colName2, alias);
        }
        else if (operator.equals("-") || operator.equals("*") || operator.equals("/")) {
            return subMulDivProject(colName1, colName2, alias, operator);
        }
        else {
            throw new RuntimeException("Invalid operator given");
        }
    }



    public static Table appendTables(Table t1, Table t2) {
        Header h1 = new Header(t1.headers);
        Header h2 = new Header(t2.headers);
        Header finalHeader = Header.joinHeaders(h1, h2);

        Table finalTable = new Table(finalHeader.numCols);
        finalTable.headers = finalHeader;

        int t1NumRows = t1.rowView.size();
        int t2NumRows = t2.rowView.size();

        if (t1NumRows != t2NumRows) {
            throw new RuntimeException("Trying to append tables of different sizes");
        }

        for (int i = 0; i < t1NumRows; i += 1) {
            Row r = Row.joinRows(t1.rowView.get(i), t2.rowView.get(i));
            finalTable.rowView.add(r);
        }

        return finalTable;
    }

    public static Table appendTables(Table[] tableList) {
        if (tableList.length == 1 || tableList[1] == null) {
            return new Table(tableList[0]);
        }
        else{
            Table[] restOfTableList = Arrays.copyOfRange(tableList, 1, tableList.length);
            return appendTables(tableList[0], appendTables(restOfTableList));
        }
    }

    public Table binaryWhere(String colName1, String colName2, String comparator) {
        if (comparator.equals(">")) {
            return  binaryWhereGreater(colName1, colName2);
        }
        else if (comparator.equals(">=")) {
            return  binaryWhereGreaterEquals(colName1, colName2);
        }
        else if (comparator.equals("<")) {
            return  binaryWhereLesser(colName1, colName2);
        }
        else if (comparator.equals("<=")) {
            return  binaryWhereLesserEquals(colName1, colName2);
        }
        else if (comparator.equals("==")) {
            return  binaryWhereEquals(colName1, colName2);
        }
        else if (comparator.equals("!=")) {
            return  binaryWhereNotEquals(colName1, colName2);
        }
        else {
            throw new RuntimeException("Invalid comparator");
        }
    }

    public Table binaryWhereGreater(String colName1, String colName2) {
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(r.data.get(colName2)) > 0 && (!r.data.get(colName1).val.equals("NOVALUE")) && (!r.data.get(colName2).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;

    }

    public Table binaryWhereLesser(String colName1, String colName2) {
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(r.data.get(colName2)) < 0 && (!r.data.get(colName1).val.equals("NOVALUE")) && (!r.data.get(colName2).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table binaryWhereEquals(String colName1, String colName2) {
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(r.data.get(colName2)) == 0 && (!r.data.get(colName1).val.equals("NOVALUE")) && (!r.data.get(colName2).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table binaryWhereGreaterEquals(String colName1, String colName2) {
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(r.data.get(colName2)) >= 0 && (!r.data.get(colName1).val.equals("NOVALUE")) && (!r.data.get(colName2).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table binaryWhereLesserEquals(String colName1, String colName2) {
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(r.data.get(colName2)) <= 0 && (!r.data.get(colName1).val.equals("NOVALUE")) && (!r.data.get(colName2).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table binaryWhereNotEquals(String colName1, String colName2) {
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(r.data.get(colName2)) != 0 && (!r.data.get(colName1).val.equals("NOVALUE")) && (!r.data.get(colName2).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table unaryWhere(String colName1, String secondVal, String comparator) {
        if (comparator.equals(">")) {
            return unaryWhereGreater(colName1, secondVal);
        }
        else if (comparator.equals(">=")) {
            return unaryWhereGreaterEquals(colName1, secondVal);
        }
        else if (comparator.equals("<")) {
            return unaryWhereLesser(colName1, secondVal);
        }
        else if (comparator.equals("<=")) {
            return unaryWhereLesserEquals(colName1, secondVal);
        }
        else if (comparator.equals("==")) {
            return unaryWhereEquals(colName1, secondVal);
        }
        else if (comparator.equals("!=")) {
            return unaryWhereNotEquals(colName1, secondVal);
        }
        else {
            throw new RuntimeException("Invalid comparator");
        }
    }

    public Table unaryWhereGreater(String colName1, String secondVal) {
        Element secondElement = new Element(secondVal, Element.detectType(secondVal));
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(secondElement) > 0 && (!r.data.get(colName1).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table unaryWhereGreaterEquals(String colName1, String secondVal) {
        Element secondElement = new Element(secondVal, Element.detectType(secondVal));
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(secondElement) >= 0 && (!r.data.get(colName1).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table unaryWhereLesser(String colName1, String secondVal) {
        Element secondElement = new Element(secondVal, Element.detectType(secondVal));
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(secondElement) < 0 && (!r.data.get(colName1).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table unaryWhereLesserEquals(String colName1, String secondVal) {
        Element secondElement = new Element(secondVal, Element.detectType(secondVal));
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(secondElement) <= 0 && (!r.data.get(colName1).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table unaryWhereEquals(String colName1, String secondVal) {
        Element secondElement = new Element(secondVal, Element.detectType(secondVal));
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(secondElement) == 0 && (!r.data.get(colName1).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public Table unaryWhereNotEquals(String colName1, String secondVal) {
        Element secondElement = new Element(secondVal, Element.detectType(secondVal));
        Table returnTable = new Table(this.numCols);
        Header newHeader = new Header(this.headers);
        returnTable.headers = newHeader;
        for (Row r : this.rowView) {
            if (r.data.get(colName1).compareTo(secondElement) != 0 && (!r.data.get(colName1).val.equals("NOVALUE"))) {
                returnTable.rowView.add(new Row(r));
            }
        }
        return returnTable;
    }

    public List<String> getCol(String colName) {
        List<String> column = new ArrayList<>();
        for (Row r: rowView) {
            column.add(r.data.get(colName).val);
        }
        return column;
    }

    public String toString() {
        String tbl = "";
        tbl += headers.toString() + "\n";
        for (Row r : rowView) {
            tbl += (r.toString() + "\n");
        }
        return tbl;
    }

    public void printTable() {
        System.out.printf(this.toString());
    }

    public static <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();

        for (T t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

    public static <T> List<T> difference(List<T> list1, List<T> list2) {
        //difference list1 - list2
        List<T> diff = new ArrayList<>(list1.size());
        diff.addAll(list1);
        diff.removeAll(list2);
        return diff;
    }
}
