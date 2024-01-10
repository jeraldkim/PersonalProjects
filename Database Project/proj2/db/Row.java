package db;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Row {
    public int size;
    public Header headers;
    public Map<String, Element> data = new HashMap<>();

    public Row() {
        this.headers = null; // row's header is set to null
        this.data = new HashMap<>();
        this.size = 0;
    }

    public Row(String line, Header h) {
        String[] vals = line.split(",");
        size = vals.length;
        headers = new Header(h);
        data = new HashMap<>();
        List<String> colNames = headers.getColNames();
        for (int i = 0; i < colNames.size(); i += 1) {
            Element newElement = new Element(vals[i], headers.getType(colNames.get(i)));
            data.put(colNames.get(i), newElement);
        }
    }

    public Row(String[] vals, Header h) {
        size = vals.length;
        headers = h;
        data = new HashMap<>();
        for (int i = 0; i < size; i += 1) {
            data.put(h.getIVal(i), new Element(vals[i], h.getIType(i)));
        }
    }

    public Row(Row r) {
        size = r.size;
        headers = new Header(r.headers);
        for (int i = 0; i < size; i += 1) {
            data.put(headers.getIVal(i), new Element(r.data.get(headers.getIVal(i))));
        }
    }

    public String toString() {
        String rv = "";
        for (int i = 0; i < this.size - 1; i += 1) {
            rv = rv + (data.get(headers.getIVal(i)).val + ",");
        }
        //trying to debug for god product
//        System.out.println(this.size);
//        System.out.println(this.size - 1); //error row size is set to 0 , leading to -1 *fixed but still errored
//        System.out.println(headers); // header is null
//        System.out.println(headers.getIVal(this.size - 1));
//        System.out.println(data.get(headers.getIVal(this.size - 1)));
//        System.out.println(data.get(headers.getIVal(this.size - 1)).val);
        rv = rv + (data.get(headers.getIVal(this.size - 1)).val);
        return rv;
    }

    public static Row joinRows (Row r1, Row r2) {
        Row newRow = new Row();
        newRow.headers = Header.joinHeaders(r1.headers, r2.headers);

        newRow.data = new HashMap<>();
        newRow.data.putAll(r1.data);
        newRow.data.putAll(r2.data);

        newRow.size = r1.size + r2.size;

        return newRow;
    }

    public Row subRow(String[] colNames) {
        Row newRow = new Row();

        newRow.headers = Header.subHeader(colNames, this.headers);
        newRow.size = colNames.length;

        for (String colName: colNames) {
            Element newElement = new Element(this.data.get(colName).val, this.headers.getType(colName));
            newRow.data.put(colName, newElement);
        }
        return newRow;
    }

    public Row removeCols(String[] colNames) {
        Row r = new Row(this);
        for (String colName : colNames) {
            r.data.remove(colName);
            r.size -= 1;
        }

        r.headers = Header.removeCols(colNames, r.headers);
        return r;
    }

    public boolean satisfiesCond(String colName, String val) {
        return data.get(colName).val.equals(val);
    }

    public static boolean willJoin(List<String> colNames, Row r1, Row r2) {
        for (String colName : colNames) {
            if (!(r1.data.get(colName).val).equals((r2.data.get(colName).val))) {
                return false;
            }
        }
        return true;
    }

    public static Row natJoin(String[] commonCols, Row r1, Row r2) {
        Row r = joinRows(r1.subRow(commonCols), r1.removeCols(commonCols));
        return joinRows(r, r2.removeCols(commonCols));
    }

    public static Row natJoin(List<String> commonCols, Row r1, Row r2) {
        String[] commonCols1 = commonCols.toArray(new String[commonCols.size()]);

        Row R1 = r1.subRow(commonCols1);
        Row R2 = r1.removeCols(commonCols1);
        Row R3 = r2.removeCols(commonCols1);

        return (Row.joinRows(R1, Row.joinRows(R2, R3)));
    }

}
