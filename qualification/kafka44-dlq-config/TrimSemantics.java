import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.internals.AppInfoParser;

/** Executes the released parser, not a local trim reimplementation. */
public class TrimSemantics {
    public static void main(String[] args) {
        System.out.println("Kafka=" + AppInfoParser.getVersion());
        String[][] cases = {
            {" \tdlq.\r\n", "dlq."},
            {"\u0000dlq.\u001f", "dlq."},
            {"\u00a0dlq.\u00a0", "\u00a0dlq.\u00a0"},
            {"\u2003", "\u2003"}
        };
        for (String key : new String[]{"errors.deadletterqueue.topic.name.prefix", "errors.deadletterqueue.topic.name"}) {
            for (int i = 0; i < cases.length; i++) {
                Object actual = ConfigDef.parseType(key, cases[i][0], ConfigDef.Type.STRING);
                if (!cases[i][1].equals(actual)) throw new AssertionError(key + " case " + i);
                System.out.println(key + " case=" + i + " PASS codepoints=" + ((String)actual).codePoints().mapToObj(c -> String.format("U+%04X", c)).toList());
            }
        }
    }
}
