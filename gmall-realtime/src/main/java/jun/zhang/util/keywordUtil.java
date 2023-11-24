package jun.zhang.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class keywordUtil {
    public static List<String> splitKeyword(String keyword) throws IOException {
        //创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();
        //创建IK分词对象  ik_smart  ik_max_word
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);
        //循环取出切分好的值
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }
        //最终返回集合
        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("撒扩大解答解答达到dadadada"));
    }
}
