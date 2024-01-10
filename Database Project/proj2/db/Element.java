package db;


public class Element {
    public String val;
    public String type;

    public Element(String v, String t) { // changed ELEMENT CONSTRUCTOR
        v = v.trim();
        t = t.trim();
        val = v;
        if (t.equals("float")) {
            if (v.equals("NOVALUE")) {
                val = "NOVALUE";
            } else {
                float floatVal = Float.parseFloat(v);
                val = String.format("%.3f", floatVal);
            }
        }
//        } else if (t.equals("int")) {
//            if (v.equals("NOVALUE")) {
//                val = "NOVALUE";
//            }
//        }
        type = t;
    }

    public Element(Element e) {
        val = e.val;
        type = e.type;
    }

    public String toString() {
        if (type.equals("float")) {
            return String.format("%.3f", val);
        }
        return val;
    }

    public boolean equals(Element other) {
        return this.val == other.val && this.type == other.type;
    }

    public float compareTo(Element other) {
        if (val.equals("NaN")) {
            if(other.val.equals("NaN")) {
                return 0;
            }
            else{
                return 1;
            }
        } else {
            if(other.val.equals("NaN")) {
                return -1;
            }
        }

        if (type.equals("string") && other.type.equals("string")) {
            String str1 = val.substring(1,val.length() - 1);
            String str2 = other.val.substring(1,other.val.length() - 1);
            return str1.compareTo(str2);
        }
        else if ((type.equals("int") || type.equals("float")) && (other.type.equals("int") || other.type.equals("float"))) {
            //fixing bug for subtracting NOVALUES
            String finalval1 = val;
            String finalval2 = other.val;
            if (finalval1.equals("NOVALUE")) {
                finalval1 = "0";
            } else if (finalval2.equals("NOVALUE")) {
                finalval2 = "0";
            }
            return Float.parseFloat(finalval1) - Float.parseFloat(finalval2);
        }
        else {
            throw new RuntimeException("Uncomparable types: " + val + " " + type + "," + " " + other.val + other.type);
        }
    }

    public static String detectType(String val) {
        try {
            int i = Integer.parseInt(val);
            return "int";
        } catch (NumberFormatException e) {
            try {
                float f = Float.parseFloat(val);
                return "float";
            } catch (NumberFormatException ne){
                return "string";
            }
        }
    }

    public Element arithmetic(Element other, String operator) {
        if (operator.equals("+")){
            return this.add(other);
        }
        else if (operator.equals("-")){
            return this.sub(other);
        }
        else if (operator.equals("*")){
            return this.mul(other);
        }
        else if (operator.equals("/")){
            return this.div(other);
        }
        else {
            throw new RuntimeException("unknown operator: " + operator + " provided");
        }
    }

    public Element add(Element other) {
        if (val.equals("NOVALUE") && other.val.equals("NOVALUE")) {
            return new Element("NOVALUE", type);
        }
        else if (val.equals("NOVALUE")) {
            if (type.equals("string")) {
                val = "";
            } else if (type.equals("int")) {
                val = "0";
            } else {
                val = "0.0";
            }
            add(other);
        } else if (other.val.equals("NOVALUE")) {
            if (other.type.equals("string")) {
                other.val = "";
            } else if (type.equals("int")) {
                other.val = "0";
            } else {
                other.val = "0.0";
            }
            add(other);
        }

        if (val.equals("NaN")) {
            return new Element("NaN", type);
        }
        else if (other.val.equals("NaN")) {
            return new Element("NaN", other.type);
        }
        else if (type.equals("string") && other.type.equals("string")){
            String str1 = val.substring(0, val.length() - 1);
            String str2 = other.val.substring(1, other.val.length());
            return new Element(str1 + str2, "string");
        }
        else if (type.equals("int") && other.type.equals("int")) {
            int first = Integer.parseInt(val);
            int second = Integer.parseInt(other.val);
            String rvString = Integer.toString(first + second);
            return new Element(rvString, "int");
        }
        else if ((type.equals("int") || type.equals("float")) && ((other.type.equals("int") || other.type.equals("float")))) {
            float first = Float.parseFloat(val);
            float second = Float.parseFloat(other.val);
            String rvString = Float.toString(first + second);
            return new Element(rvString, "float");
        }
        else {
            throw new RuntimeException("trying to add incompatible types");
        }
    }
    public Element sub(Element other) {
        if (val.equals("NOVALUE") && other.val.equals("NOVALUE")) {
            return new Element("NOVALUE", type);
        }
        else if (val.equals("NOVALUE")) { //potential problem is changing novalue's value to a different value, changing the string representation from NOVALUE TO 0
            if (type.equals("string")) {
                val = "";
            } else if (type.equals("int")) {
                val = "0";
            } else {
                val = "0.0";
            }
            sub(other);
        } else if (other.val.equals("NOVALUE")) {
            if (other.type.equals("string")) {
                other.val = "";
            } else if (type.equals("int")) {
                other.val = "0";
            } else {
                other.val = "0.0";
            }
            sub(other);
        }

        if (val.equals("NaN")) {
            return new Element("NaN", type);
        }
        else if (other.val.equals("NaN")) {
            return new Element("NaN", other.type);
        }
        else if (type.equals("int") && other.type.equals("int")) {
            int first = Integer.parseInt(val);
            int second = Integer.parseInt(other.val);
            String rvString = Integer.toString(first - second);
            return new Element(rvString, "int");
        }
        else if ((type.equals("int") || type.equals("float")) && ((other.type.equals("int") || other.type.equals("float")))) {
            float first = Float.parseFloat(val);
            float second = Float.parseFloat(other.val);
            String rvString = Float.toString(first - second);
            return new Element(rvString, "float");
        }
        else {
            throw new RuntimeException("trying to add incompatible types");
        }
    }
    public Element mul(Element other) {
        if (val.equals("NOVALUE") && other.val.equals("NOVALUE")) {
            return new Element("NOVALUE", type);
        }
        else if (val.equals("NOVALUE")) {
            if (type.equals("string")) {
                val = "";
            } else if (type.equals("int")) {
                val = "0";
            } else {
                val = "0.0";
            }
            mul(other);
        } else if (other.val.equals("NOVALUE")) {
            if (other.type.equals("string")) {
                other.val = "";
            } else if (type.equals("int")) {
                other.val = "0";
            } else {
                other.val = "0.0";
            }
            mul(other);
        }

        if (val.equals("NaN")) {
            return new Element("NaN", type);
        }
        else if (other.val.equals("NaN")) {
            return new Element("NaN", other.type);
        }
        else if (type.equals("int") && other.type.equals("int")) {
            int first = Integer.parseInt(val);
            int second = Integer.parseInt(other.val);
            String rvString = Integer.toString(first * second);
            return new Element(rvString, "int");
        }
        else if ((type.equals("int") || type.equals("float")) && ((other.type.equals("int") || other.type.equals("float")))) {
            float first = Float.parseFloat(val);
            float second = Float.parseFloat(other.val);
            String rvString = Float.toString(first * second);
            return new Element(rvString, "float");
        }
        else {
            throw new RuntimeException("trying to add incompatible types");
        }
    }
    public Element div(Element other) {
        if (val.equals("NOVALUE") && other.val.equals("NOVALUE")) {
            return new Element("NOVALUE", type);
        }
        else if (val.equals("NOVALUE")) {
            if (type.equals("string")) {
                val = "";
            } else if (type.equals("int")) {
                val = "0";
            } else {
                val = "0.0";
            }
            div(other);
        } else if (other.val.equals("NOVALUE")) {
            if (other.type.equals("string")) {
                other.val = "";
            } else if (type.equals("int")) {
                other.val = "0";
            } else {
                other.val = "0.0";
            }
            div(other);
        }

        if (val.equals("NaN")) {
            return new Element("NaN", type);
        }
        else if (other.val.equals("NaN")) {
            return new Element("NaN", other.type);
        }
        else if (other.val.equals("0") && (other.type.equals("int") || other.type.equals("float"))) {
            return new Element("NaN", type);
        }
        else if (type.equals("int") && other.type.equals("int")) {
            int first = Integer.parseInt(val);
            int second = Integer.parseInt(other.val);
            String rvString = Integer.toString(first / second); //forgot to divide cp
            if (rvString.equals("Infinity")) {
                rvString = "NaN";
            }
            return new Element(rvString, "int");
        }
        else if ((type.equals("int") || type.equals("float")) && ((other.type.equals("int") || other.type.equals("float")))) {
            float first = Float.parseFloat(val);
            float second = Float.parseFloat(other.val);
            String rvString = Float.toString(first / second); //forgot to divide cp
            if (rvString.equals("Infinity")) {
                rvString = "NaN";
            }
            return new Element(rvString, "float");
        }
        else {
            throw new RuntimeException("trying to add incompatible types");
        }
    }
}
