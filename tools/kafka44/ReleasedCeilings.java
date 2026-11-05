import java.util.TreeMap;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;

/** Derives policy inputs from pinned release bytecode, without activating any native defaults. */
public class ReleasedCeilings {
    public static void main(String[] args) {
        if (args.length != 0) throw new IllegalArgumentException("no arguments");
        var metadata = MetadataVersion.latestProduction();
        System.out.println("METADATA\t" + metadata.version() + "\t" + metadata.featureLevel());
        for (var api : ApiKeys.values()) {
            api.toApiVersion(false).ifPresent(row -> System.out.println("REQUEST\t" + api.id + "\t" + api.name
                + "\t" + row.minVersion() + "\t" + row.maxVersion()));
            for (var listener : ListenerType.values())
                if (api.inScope(listener)) api.toApiVersionForApiResponse(false, listener).ifPresent(row ->
                    System.out.println("ADVERTISED\t" + listener + "\t" + api.id + "\t" + api.name
                        + "\t" + row.minVersion() + "\t" + row.maxVersion()));
        }
        for (var feature : Feature.PRODUCTION_FEATURES) {
            System.out.println("FEATURE\t" + feature.featureName() + "\t" + feature.minimumProduction()
                + "\t" + feature.latestProduction() + "\t" + feature.defaultLevel(metadata));
            for (var version : feature.featureVersions()) {
                if (feature.isProductionReady(version.featureLevel()))
                    System.out.println("DEPENDENCIES\t" + feature.featureName() + "\t" + version.featureLevel()
                        + "\t" + version.bootstrapMetadataVersion().featureLevel() + "\t" + new TreeMap<>(version.dependencies()));
            }
        }
    }
}
