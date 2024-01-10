package db;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

public class Header {
    public String[][] header;  //0th item is name 1st element is type of data
    public int numCols;

    public Header(int numCols) {
        this.header = new String[numCols][2];
        this.numCols = numCols;
    }

    public Header(String line) {
        String[] headerList = line.split(",");
        numCols = headerList.length;
        header = new String[numCols][2];

        for (int i = 0; i < headerList.length; i += 1) {
            String h = headerList[i];
            h = h.trim();
            String[] splits = h.split(" "); // may not fix arbitrary number of spaces
            header[i][0] = h.split(" ")[0];
            header[i][1] = h.split(" ")[1];
        }
    }

    public Header(Header h) {
        numCols = h.numCols;
        header = new String[numCols][2];
        for (int i = 0; i < numCols; i += 1) {
            header[i][0] = h.header[i][0];
            header[i][1] = h.header[i][1];
        }
    }

    public static Header subHeader(String[] colNames, Header h) {
        Header newHeader = new Header(colNames.length);
        for (int i = 0; i < colNames.length; i += 1) {
            newHeader.header[i][0] = colNames[i];
            newHeader.header[i][1] = h.getType(colNames[i]);
        }
        return newHeader;
    }

    public static Header subHeader(List<String> colNames, Header h) {
        String[] colNames1 = colNames.toArray(new String[colNames.size()]);
        return subHeader(colNames1, h);
    }

    public int getPos(String val) {
        for (int i = 0; i < header.length; i += 1) {
            if (header[i][0].equals(val)) {
                return i;
            }
            throw new RuntimeException("column name not found in header list");
        }
        return -1;
    }

    public String getIVal (int i) {
        return header[i][0];
    }

    public String getIType (int i) {
        return header[i][1];
    }

    public String getType (String colName) {
        for (int i = 0; i < numCols; i += 1) {
            if (header[i][0].equals(colName)) {
                return header[i][1];
            }
        }
        return "Column not found in header";
    }

    public void setIType (int i, String type) {
        header[i][1] = type;
    }

    public void setIVal (int i, String val) {
        header[i][0] = val;
    }

    public void setValType (String val, String type) {
        int i = getPos(val);
        setIType(i, type);
    }

    public String toString() {
        String rv = "";
        for (int i = 0; i < numCols - 1; i += 1) {
            rv = rv + (getIVal(i) + " " + getIType(i) + ",");
        }
        rv = rv + (getIVal(numCols - 1) + " " + getIType(numCols - 1));
        return rv;
    }

    public static Header joinHeaders(Header h1, Header h2) {
        Header newHeader = new Header(h1.numCols + h2.numCols);
        for (int i = 0; i < h1.numCols; i += 1) {
            newHeader.header[i][0] = h1.header[i][0];
            newHeader.header[i][1] = h1.header[i][1];
        }
        for (int i = 0; i < h2.numCols; i += 1) {
            newHeader.header[h1.numCols + i][0] = h2.header[i][0];
            newHeader.header[h1.numCols + i][1] = h2.header[i][1];
        }
        return newHeader;
    }

    public List<String> getColNames() {
        String[] cols = new String[this.numCols];
        for (int i = 0; i < numCols; i += 1) {
            cols[i] = header[i][0];
        }

        List<String> rv = new ArrayList<String>(Arrays.asList(cols));

        return rv;
    }

    public static Header removeCols(String[] colNames, Header h) {
        Header newHeader = new Header(h.numCols - colNames.length);
        newHeader.header = new String[h.numCols - colNames.length][2];
        int i = 0;
        for (String[] col : h.header) {
            if (!contains(colNames, col[0])) {
                newHeader.setIVal(i, col[0]);
                newHeader.setIType(i, col[1]);
                i += 1;
            }
        }
        return newHeader;
    }
    public static Header removeCols(List<String> colNames, Header h) {
        String[] colNames1 = colNames.toArray(new String[colNames.size()]);
        return removeCols(colNames1, h);
    }

    public static List<String> commonCols(Header h1, Header h2) {
        List<String> t1HeaderList = h1.getColNames();
        List<String> t2HeaderList = h2.getColNames();
        return intersection(t1HeaderList, t2HeaderList);
    }

    public static Header natJoinHeaders(Header h1, Header h2) {
        List<String> commonCols = commonCols(h1, h2);
        Header first = h1.subHeader(commonCols, h1);
        Header second = Header.removeCols(commonCols, h1);
        Header third = Header.removeCols(commonCols, h2);
        return Header.joinHeaders(first, Header.joinHeaders(second, third));
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

    public static <T> boolean contains(final T[] array, final T v) {
        if (v == null) {
            for (final T e : array)
                if (e == null)
                    return true;
        } else {
            for (final T e : array)
                if (e == v || v.equals(e))
                    return true;
        }

        return false;
    }

}
