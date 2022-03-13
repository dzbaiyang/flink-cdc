package com;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang.StringEscapeUtils;

public class test {
    public static void main(String[] args) {{
            String str1 = "";
            String tmp = StringEscapeUtils.unescapeJavaScript(str1);
            System.out.println("tmp:" + tmp);
    }
    }
}

