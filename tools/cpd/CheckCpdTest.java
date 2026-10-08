package net.sourceforge.pmd.cpd;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Small real-lexer/native-CPD fixtures, including matches hidden by PMD's pruning. */
public final class CheckCpdTest {
    private static int passed;

    private CheckCpdTest() { }

    public static void main(String[] args) throws Exception {
        int t = CheckCpd.MINIMUM_TOKENS;
        String atThreshold = tokens("p", t);
        check("one token below the threshold", List.of("old"), List.of(tokens("p", t - 1), tokens("p", t - 1)), 0);
        check("at the threshold", List.of("old"), List.of(atThreshold, atThreshold), 1);
        check("unchanged", List.of(atThreshold, atThreshold), List.of(atThreshold, atThreshold), 0);
        check("comments and line moves", List.of(atThreshold, atThreshold),
                List.of("\n// moved\n" + atThreshold.replace(" ", " /* note */\n"), "\n\n" + atThreshold), 0);
        check("third copy", List.of(atThreshold, atThreshold), List.of(atThreshold, atThreshold, atThreshold), 1);
        check("remove unrelated repeat", List.of(atThreshold, atThreshold),
                List.of(tokens("new", t), tokens("new", t)), 1);
        check("shrinking old match", List.of(tokens("p", t + t / 2), tokens("p", t + t / 2)), List.of(atThreshold, atThreshold), 0);
        check("split old matches", List.of(tokens("p", 2 * t), tokens("p", 2 * t)),
                List.of(atThreshold, atThreshold, tokens("p", t, t), tokens("p", t, t)), 0);
        String middle = tokens("m", t - 1);
        check("extension with unchanged threshold-window counts", List.of("x " + middle, "x " + middle,
                middle + " y", middle + " y"), List.of("x " + middle + " y", "x " + middle + " y"), 1);
        String longer = tokens("long", t + t / 2);
        check("nested third copy", List.of(longer, longer, tokens("long", t), tokens("long", 1, t)),
                List.of(longer, longer, tokens("long", t + 1)), 1);
        check("hidden periodic threshold", List.of("old"), List.of(atThreshold + " " + atThreshold + " p0"), 1);
        List<String> cyclic = new ArrayList<>();
        for (int phase = 0; phase < t + 1; phase++) {
            StringBuilder window = new StringBuilder();
            for (int i = 0; i < t; i++) {
                window.append("cycle").append((phase + i) % (t + 1)).append(' ');
            }
            cyclic.add(window.toString());
            cyclic.add(window.toString());
        }
        String period = tokens("cycle", t + 1);
        check("hidden periodic threshold+1 with all threshold windows supported", cyclic, List.of(period + " " + period + " cycle0"), 1);
        check("overlapping reports do not increase copies", List.of(longer, longer),
                List.of("\n" + longer + " // end\n", longer), 0);
        check("one overlapping occurrence is not duplication", List.of("old"), List.of("same ".repeat(2 * t - 1)), 0);
        check("two nonoverlapping periodic copies", List.of("old"), List.of("same ".repeat(2 * t)), 1);
        check("CPD-OFF line comment", List.of("old"), List.of("// CPD-OFF\n" + atThreshold), 2);
        check("CPD-ON block comment", List.of("old"), List.of("/* CPD-ON */ " + atThreshold), 2);
        check("ordinary CPD_START doc comment", List.of("old"), List.of("/// CPD_START\n" + atThreshold), 0);
        check("directive in string is ordinary source", List.of("old"), List.of("\"CPD-OFF\""), 0);
        check("escaped strings and byte strings", List.of("old"),
                List.of("let a = \"quote\\\" and \\\\path\"; let b = b\"quote\\\" and \\\\path\";"), 0);
        check("lexer error", List.of("old"), List.of("let x = \u0001;"), 2);
        check("hidden Rust path", List.of("old"), List.of(atThreshold, atThreshold), 1, true);
        System.out.println("CheckCpdTest: " + passed + " real PMD regression fixtures passed");
    }

    private static String tokens(String prefix, int length) { return tokens(prefix, 0, length); }
    private static String tokens(String prefix, int start, int length) {
        StringBuilder tokens = new StringBuilder();
        for (int i = start; i < start + length; i++) {
            tokens.append(prefix).append(i).append(' ');
        }
        return tokens.toString();
    }

    private static void check(String name, List<String> base, List<String> head, int expected) throws IOException {
        check(name, base, head, expected, false);
    }

    private static void check(String name, List<String> base, List<String> head, int expected, boolean hidden) throws IOException {
        Path root = Files.createTempDirectory("cpd-regression-");
        try {
            Path before = root.resolve("base");
            Path after = root.resolve("head");
            write(before, base, false);
            write(after, head, hidden);
            Path output = root.resolve("reports");
            int actual = CheckCpd.run(before, after, output);
            if (actual != expected) {
                throw new AssertionError(name + ": expected exit " + expected + ", got " + actual);
            }
            if (!Files.isRegularFile(output.resolve("base.xml")) || !Files.isRegularFile(output.resolve("head.xml"))) {
                throw new AssertionError(name + ": both native XML reports must exist");
            }
            if (name.startsWith("hidden periodic") && !Files.readString(output.resolve("head.xml")).contains("<duplication")) {
                // These fixtures intentionally prove the ratchet does not rely on native maximal-match reporting.
                System.out.println(name + ": regression detected despite zero native groups");
            }
            passed++;
        } finally {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    private static void write(Path directory, List<String> sources, boolean hidden) throws IOException {
        Files.createDirectories(directory);
        for (int i = 0; i < sources.size(); i++) {
            Path parent = hidden && i == 0 ? directory.resolve(".hidden") : directory;
            Files.createDirectories(parent);
            Files.writeString(parent.resolve("source" + i + ".rs"), sources.get(i), StandardCharsets.UTF_8);
        }
    }
}
