package net.sourceforge.pmd.cpd;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import net.sourceforge.pmd.lang.Language;
import net.sourceforge.pmd.lang.LanguageRegistry;
import net.sourceforge.pmd.lang.ast.LexException;
import net.sourceforge.pmd.lang.document.FileId;
import net.sourceforge.pmd.lang.document.TextDocument;
import net.sourceforge.pmd.lang.document.TextFile;
import net.sourceforge.pmd.lang.rust.ast.RustLexer;
import net.sourceforge.pmd.lang.rust.cpd.RustCpdLexer;
import net.sourceforge.pmd.reporting.Report;
import net.sourceforge.pmd.util.log.PmdReporter;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

/** A source-derived, exact-token ratchet accompanying native PMD Rust CPD reports. */
public final class CheckCpd {
    static final int MINIMUM_TOKENS = 100;
    private static final Language RUST = LanguageRegistry.CPD.getLanguageById("rust");
    private static final long HASH_BASE = 1_000_003L;
    private static final Pattern SUPPRESSION = Pattern.compile("CPD-(?:OFF|ON)");
    // Resource exhaustion fails the gate explicitly; it never truncates comparison.
    private static final long MAX_PAIR_VISITS = 100_000_000L;
    private static final int MAX_TRIE_NODES = 2_000_000;
    private static final long MAX_OUTPUT_VISITS = 100_000_000L;

    private CheckCpd() { }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: net.sourceforge.pmd.cpd.CheckCpd BASE_DIR HEAD_DIR REPORT_DIR");
            System.exit(2);
        }
        try {
            System.exit(run(Path.of(args[0]), Path.of(args[1]), Path.of(args[2])));
        } catch (IOException | RuntimeException e) {
            System.err.println("CPD check failed: " + e.getMessage());
            System.exit(2);
        }
    }

    static int run(Path baseDir, Path headDir, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        Symbols symbols = new Symbols();
        Corpus base = read(baseDir, symbols, outputDir.resolve("base.xml"));
        Corpus head = read(headDir, symbols, outputDir.resolve("head.xml"));
        if (!base.errors.isEmpty() || !head.errors.isEmpty()) {
            String errors = String.join("\n", base.errors) + "\n" + String.join("\n", head.errors);
            Files.writeString(outputDir.resolve("violations.txt"), errors, StandardCharsets.UTF_8);
            System.err.println(errors.strip());
            return 2;
        }
        System.out.println("Native PMD: base=" + base.files.size() + " files/" + base.groups
                + " groups; head=" + head.files.size() + " files/" + head.groups + " groups; zero lexer errors");
        Trie candidates = candidates(head);
        long[] before = candidates.count(base);
        long[] after = candidates.count(head);
        List<Integer> violations = new ArrayList<>();
        for (int i = 1; i < candidates.nodes.size(); i++) {
            Node node = candidates.nodes.get(i);
            if (node.terminal && after[i] >= 2 && after[i] > Math.max(1L, before[i])) {
                violations.add(i);
            }
        }
        Map<Integer, List<String>> locations = candidates.locations(head, violations);
        StringBuilder report = new StringBuilder();
        for (int node : violations) {
            int[] sequence = candidates.sequence(node);
            report.append(fingerprint(sequence, symbols)).append(" tokens=").append(sequence.length)
                    .append(" copies=").append(before[node]).append(" -> ").append(after[node]);
            for (String location : locations.getOrDefault(node, List.of())) {
                report.append("\n  ").append(location);
            }
            report.append('\n');
        }
        Files.writeString(outputDir.resolve("violations.txt"), report, StandardCharsets.UTF_8);
        String summary = "{\n  \"pmdVersion\": \"7.28.0\",\n  \"minimumTokens\": " + MINIMUM_TOKENS + ",\n"
                + "  \"baseFiles\": " + base.files.size() + ",\n  \"headFiles\": " + head.files.size()
                + ",\n  \"baseGroups\": " + base.groups + ",\n  \"headGroups\": " + head.groups
                + ",\n  \"lexerErrors\": 0,\n  \"candidateSequences\": " + candidates.terminals
                + ",\n  \"trieNodes\": " + candidates.nodes.size() + ",\n  \"pairVisits\": " + candidates.pairVisits
                + ",\n  \"witnessPairs\": " + candidates.pairs
                + ",\n  \"violations\": " + violations.size() + "\n}\n";
        Files.writeString(outputDir.resolve("summary.json"), summary, StandardCharsets.UTF_8);
        System.out.println("Compared " + candidates.terminals + " repeated token sequences: "
                + violations.size() + " new or increased repeats (reports: " + outputDir + ")");
        report.toString().lines().limit(24).forEach(System.err::println);
        return violations.isEmpty() ? 0 : 1;
    }

    private static Corpus read(Path directory, Symbols symbols, Path xml) throws IOException {
        Path root = directory.toAbsolutePath().normalize();
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Not a source directory: " + root);
        }
        List<Path> paths;
        try (Stream<Path> walk = Files.walk(root)) {
            paths = walk.filter(path -> path.toString().endsWith(".rs")).sorted().toList();
        }
        if (paths.isEmpty()) {
            throw new IOException("No Rust sources in " + root);
        }
        Corpus corpus = new Corpus();
        List<TextFile> textFiles = new ArrayList<>();
        Map<FileId, Integer> counts = new LinkedHashMap<>();
        List<Report.ProcessingError> errors = new ArrayList<>();
        for (Path path : paths) {
            FileId id = FileId.fromPath(path);
            try {
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Rust source is not a regular file: " + path);
                }
                String source = Files.readString(path, StandardCharsets.UTF_8);
                rejectSuppressions(source);
                TextFile text = TextFile.forPath(path, StandardCharsets.UTF_8, RUST.getDefaultVersion());
                textFiles.add(text);
                try (TextDocument document = TextDocument.create(text)) {
                    Capture capture = new Capture(document, symbols);
                    try (capture) {
                        new RustCpdLexer().tokenize(document, capture);
                    }
                    corpus.files.add(new Source(root.relativize(path).toString(),
                            Arrays.copyOf(capture.images, capture.size), Arrays.copyOf(capture.positions, capture.size)));
                    counts.put(id, capture.size);
                }
            } catch (IOException | RuntimeException error) {
                errors.add(new Report.ProcessingError(error, id));
                corpus.errors.add(path + ": " + error.getMessage());
            }
        }
        if (!errors.isEmpty()) {
            try (SourceManager sources = new SourceManager(textFiles);
                 Writer writer = Files.newBufferedWriter(xml, StandardCharsets.UTF_8)) {
                new XMLRenderer().render(new CPDReport(sources, List.of(), counts, errors), writer);
            }
            return corpus;
        }
        CPDConfiguration config = new CPDConfiguration();
        config.setMinimumTileSize(MINIMUM_TOKENS);
        config.setOnlyRecognizeLanguage(RUST);
        config.setSourceEncoding(StandardCharsets.UTF_8);
        config.setReporter(PmdReporter.quiet());
        config.setRendererName("xml");
        config.setReportFile(xml);
        config.setSkipLexicalErrors(false);
        boolean[] complete = {false};
        try (CpdAnalysis analysis = CpdAnalysis.create(config)) {
            for (Path path : paths) {
                if (!analysis.files().addFile(path, RUST)) {
                    throw new IOException("PMD refused source " + path);
                }
            }
            analysis.performAnalysis(report -> {
                if (!report.getProcessingErrors().isEmpty() || !counts.equals(report.getNumberOfTokensPerFile())) {
                    throw new IllegalStateException("PMD source/token coverage differs from the complete Rust inventory");
                }
                corpus.groups = report.getMatches().size();
                complete[0] = true;
            });
        }
        if (!complete[0] || !Files.isRegularFile(xml)) {
            throw new IOException("PMD did not finish reporting all Rust sources in " + root);
        }
        return corpus;
    }

    /** Inspect comment tokens before PMD's suppression filter can discard them. */
    private static void rejectSuppressions(String source) {
        if (!source.contains("CPD")) {
            return;
        }
        RustLexer lexer = new RustLexer(CharStreams.fromString(source));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                                    int column, String message, RecognitionException error) {
                throw new IllegalArgumentException("Rust lexer error at " + line + ":" + (column + 1) + ": " + message);
            }
        });
        for (Token token = lexer.nextToken(); token.getType() != Token.EOF; token = lexer.nextToken()) {
            if (token.getChannel() != Token.DEFAULT_CHANNEL && SUPPRESSION.matcher(token.getText()).find()) {
                throw new IllegalArgumentException("CPD suppression directive at " + token.getLine()
                        + ":" + (token.getCharPositionInLine() + 1));
            }
        }
    }

    /**
     * Native CPD prunes overlapping/maximal matches, so its XML is not an occurrence index.
     * Every real nonoverlapping repeated sequence has a pair of equal initial MINIMUM_TOKENS-token
     * windows. Extending each such pair (capped at their distance within one file) exposes
     * every repeated prefix. The trie stores that complete set without copying prefixes.
     */
    private static Trie candidates(Corpus head) {
        LongIndex first = new LongIndex();
        Map<Long, List<Window>> repeated = new HashMap<>();
        long power = 1;
        for (int i = 1; i < MINIMUM_TOKENS; i++) {
            power *= HASH_BASE;
        }
        for (int file = 0; file < head.files.size(); file++) {
            int[] tokens = head.files.get(file).tokens;
            long hash = 0;
            for (int i = 0; i < tokens.length; i++) {
                if (i >= MINIMUM_TOKENS) {
                    hash -= (tokens[i - MINIMUM_TOKENS] + 1L) * power;
                }
                hash = hash * HASH_BASE + tokens[i] + 1L;
                if (i < MINIMUM_TOKENS - 1) {
                    continue;
                }
                long position = position(file, i + 1 - MINIMUM_TOKENS);
                long previous = first.putIfAbsent(hash, position + 1);
                if (previous == 0) {
                    continue;
                }
                List<Window> bucket = repeated.computeIfAbsent(hash, ignored -> {
                    List<Window> windows = new ArrayList<>();
                    windows.add(new Window(previous - 1));
                    return windows;
                });
                Window match = null;
                for (Window window : bucket) {
                    if (equal(head, window.starts.get(0), position, MINIMUM_TOKENS)) {
                        match = window;
                        break;
                    }
                }
                if (match == null) {
                    bucket.add(new Window(position)); // A hash collision is a separate exact sequence.
                } else {
                    match.starts.add(position);
                }
            }
        }
        Trie trie = new Trie();
        for (List<Window> bucket : repeated.values()) {
            for (Window window : bucket) {
                int[] longest = new int[window.starts.size()];
                for (int i = 0; i < longest.length; i++) {
                    long a = window.starts.get(i);
                    int[] left = head.files.get(file(a)).tokens;
                    for (int j = i + 1; j < longest.length; j++) {
                        if (++trie.pairVisits > MAX_PAIR_VISITS) {
                            throw new IllegalStateException("CPD pair-work budget exceeded: " + trie.pairVisits
                                    + " visits; exact comparison did not complete");
                        }
                        long b = window.starts.get(j);
                        int[] right = head.files.get(file(b)).tokens;
                        int cap = Math.min(left.length - start(a), right.length - start(b));
                        if (file(a) == file(b)) {
                            cap = Math.min(cap, Math.abs(start(a) - start(b)));
                        }
                        if (cap < MINIMUM_TOKENS || cap <= Math.min(longest[i], longest[j])) {
                            continue;
                        }
                        trie.pairs++;
                        int length = MINIMUM_TOKENS;
                        while (length < cap && left[start(a) + length] == right[start(b) + length]) {
                            length++;
                        }
                        longest[i] = Math.max(longest[i], length);
                        longest[j] = Math.max(longest[j], length);
                    }
                }
                for (int i = 0; i < longest.length; i++) {
                    if (longest[i] >= MINIMUM_TOKENS) {
                        long occurrence = window.starts.get(i);
                        trie.insert(head.files.get(file(occurrence)).tokens, start(occurrence), longest[i]);
                    }
                }
            }
        }
        trie.buildFailures();
        return trie;
    }

    private static long position(int file, int start) { return ((long) file << 32) | start; }
    private static int file(long position) { return (int) (position >>> 32); }
    private static int start(long position) { return (int) position; }

    private static boolean equal(Corpus corpus, long a, long b, int length) {
        return Arrays.equals(corpus.files.get(file(a)).tokens, start(a), start(a) + length,
                corpus.files.get(file(b)).tokens, start(b), start(b) + length);
    }

    private static String fingerprint(int[] sequence, Symbols symbols) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int token : sequence) {
                byte[] image = symbols.images.get(token).getBytes(StandardCharsets.UTF_8);
                digest.update((byte) (image.length >>> 24));
                digest.update((byte) (image.length >>> 16));
                digest.update((byte) (image.length >>> 8));
                digest.update((byte) image.length);
                digest.update(image);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class Symbols {
        final Map<String, Integer> ids = new HashMap<>();
        final List<String> images = new ArrayList<>();
        int intern(String image) {
            return ids.computeIfAbsent(image, key -> { images.add(key); return images.size() - 1; });
        }
    }

    /** Capture native images on insertion: PMD's reverse getImage lookup is linear. */
    private static final class Capture implements TokenFactory {
        final TokenFactory delegate;
        final Symbols symbols;
        int[] images = new int[128];
        long[] positions = new long[128];
        int size;
        Capture(TextDocument document, Symbols symbols) {
            this.delegate = Tokens.factoryForFile(document, new Tokens());
            this.symbols = symbols;
        }
        @Override
        public void recordToken(String image, int beginLine, int beginColumn, int endLine, int endColumn) {
            delegate.recordToken(image, beginLine, beginColumn, endLine, endColumn);
            if (size == images.length) {
                images = Arrays.copyOf(images, Math.multiplyExact(size, 2));
                positions = Arrays.copyOf(positions, images.length);
            }
            images[size] = symbols.intern(image);
            positions[size++] = ((long) beginLine << 32) | beginColumn;
        }
        @Override
        public void setImage(TokenEntry entry, String image) {
            delegate.setImage(entry, image);
            images[entry.getIndex()] = symbols.intern(image);
        }
        @Override
        public LexException makeLexException(int line, int column, String message, Throwable cause) {
            return delegate.makeLexException(line, column, message, cause);
        }
        @Override
        public TokenEntry peekLastToken() { return delegate.peekLastToken(); }
        @Override
        public void close() { delegate.close(); }
    }

    private record Source(String path, int[] tokens, long[] positions) { }
    private static final class Corpus {
        final List<Source> files = new ArrayList<>();
        final List<String> errors = new ArrayList<>();
        int groups;
    }
    private static final class Window {
        final List<Long> starts = new ArrayList<>();
        Window(long first) { starts.add(first); }
    }

    /** Primitive open-addressing index: the vast majority of MINIMUM_TOKENS-token windows occur once. */
    private static final class LongIndex {
        long[] keys = new long[1024];
        long[] values = new long[1024];
        int size;
        long putIfAbsent(long key, long value) {
            if (size * 10L >= keys.length * 7L) {
                long[] oldKeys = keys;
                long[] oldValues = values;
                keys = new long[Math.multiplyExact(keys.length, 2)];
                values = new long[keys.length];
                size = 0;
                for (int i = 0; i < oldKeys.length; i++) {
                    if (oldValues[i] != 0) {
                        putIfAbsent(oldKeys[i], oldValues[i]);
                    }
                }
            }
            long mixed = key;
            mixed = (mixed ^ (mixed >>> 33)) * 0xff51afd7ed558ccdL;
            mixed = (mixed ^ (mixed >>> 33)) * 0xc4ceb9fe1a85ec53L;
            int slot = (int) (mixed ^ (mixed >>> 33)) & (keys.length - 1);
            while (values[slot] != 0 && keys[slot] != key) {
                slot = (slot + 1) & (keys.length - 1);
            }
            if (values[slot] != 0) {
                return values[slot];
            }
            keys[slot] = key;
            values[slot] = value;
            size++;
            return 0;
        }
    }

    private static final class Node {
        final Map<Integer, Integer> edges = new HashMap<>();
        final int parent, token, depth;
        int failure, output;
        boolean terminal;
        Node(int parent, int token, int depth) { this.parent = parent; this.token = token; this.depth = depth; }
    }

    private static final class Trie {
        final List<Node> nodes = new ArrayList<>(List.of(new Node(0, 0, 0)));
        long pairs, pairVisits;
        int terminals;
        void insert(int[] tokens, int start, int length) {
            int state = 0;
            for (int i = 0; i < length; i++) {
                Node node = nodes.get(state);
                Integer next = node.edges.get(tokens[start + i]);
                if (next == null) {
                    if (nodes.size() >= MAX_TRIE_NODES) {
                        throw new IllegalStateException("CPD trie budget exceeded: " + nodes.size()
                                + " nodes; exact comparison did not complete");
                    }
                    next = nodes.size();
                    node.edges.put(tokens[start + i], next);
                    nodes.add(new Node(state, tokens[start + i], i + 1));
                }
                state = next;
                Node child = nodes.get(state);
                if (i + 1 >= MINIMUM_TOKENS && !child.terminal) {
                    child.terminal = true;
                    terminals++;
                }
            }
        }
        void buildFailures() {
            ArrayDeque<Integer> queue = new ArrayDeque<>(nodes.get(0).edges.values());
            while (!queue.isEmpty()) {
                int state = queue.remove();
                Node node = nodes.get(state);
                for (Map.Entry<Integer, Integer> edge : node.edges.entrySet()) {
                    int failure = step(node.failure, edge.getKey());
                    Node child = nodes.get(edge.getValue());
                    child.failure = failure;
                    child.output = nodes.get(failure).terminal ? failure : nodes.get(failure).output;
                    queue.add(edge.getValue());
                }
            }
        }
        int step(int state, int token) {
            while (state != 0 && !nodes.get(state).edges.containsKey(token)) {
                state = nodes.get(state).failure;
            }
            return nodes.get(state).edges.getOrDefault(token, 0);
        }
        long[] count(Corpus corpus) {
            long[] counts = new long[nodes.size()];
            int[] lastEnd = new int[nodes.size()];
            int[] lastFile = new int[nodes.size()];
            long visits = 0;
            for (int file = 0; file < corpus.files.size(); file++) {
                Source source = corpus.files.get(file);
                int state = 0;
                for (int i = 0; i < source.tokens.length; i++) {
                    state = step(state, source.tokens[i]);
                    int match = nodes.get(state).terminal ? state : nodes.get(state).output;
                    while (match != 0) {
                        if (++visits > MAX_OUTPUT_VISITS) {
                            throw new IllegalStateException("CPD occurrence-work budget exceeded: " + visits
                                    + " emissions; exact comparison did not complete");
                        }
                        Node node = nodes.get(match);
                        if (lastFile[match] != file + 1 || i + 1 - node.depth >= lastEnd[match]) {
                            counts[match]++;
                            lastEnd[match] = i + 1;
                            lastFile[match] = file + 1;
                        }
                        match = node.output;
                    }
                }
            }
            return counts;
        }
        Map<Integer, List<String>> locations(Corpus corpus, List<Integer> violations) {
            if (violations.isEmpty()) {
                return Map.of();
            }
            Map<Integer, List<String>> locations = new HashMap<>();
            for (int node : violations) {
                locations.put(node, new ArrayList<>());
            }
            int[] lastEnd = new int[nodes.size()];
            int[] lastFile = new int[nodes.size()];
            long visits = 0;
            for (int file = 0; file < corpus.files.size(); file++) {
                Source source = corpus.files.get(file);
                int state = 0;
                for (int i = 0; i < source.tokens.length; i++) {
                    state = step(state, source.tokens[i]);
                    int match = nodes.get(state).terminal ? state : nodes.get(state).output;
                    while (match != 0) {
                        if (++visits > MAX_OUTPUT_VISITS) {
                            throw new IllegalStateException("CPD diagnostic-work budget exceeded: " + visits
                                    + " emissions; exact comparison did not complete");
                        }
                        Node node = nodes.get(match);
                        List<String> found = locations.get(match);
                        int start = i + 1 - node.depth;
                        if (found != null && found.size() < 3
                                && (lastFile[match] != file + 1 || start >= lastEnd[match])) {
                            long position = source.positions[start];
                            found.add(source.path + ":" + (position >>> 32) + ":" + (int) position);
                            lastEnd[match] = i + 1;
                            lastFile[match] = file + 1;
                        }
                        match = node.output;
                    }
                }
            }
            return locations;
        }
        int[] sequence(int state) {
            int[] sequence = new int[nodes.get(state).depth];
            while (state != 0) {
                Node node = nodes.get(state);
                sequence[node.depth - 1] = node.token;
                state = node.parent;
            }
            return sequence;
        }
    }
}
