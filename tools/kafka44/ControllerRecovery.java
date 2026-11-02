import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.UnregisterControllerRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.ControllerRegistrationResponse;
import org.apache.kafka.common.requests.UnregisterControllerResponse;
import org.apache.kafka.common.requests.UnregisterControllerRequest;

/** Nonvoter removal durability fixture; prepare, restart owned broker, then recover. */
public class ControllerRecovery {
    private static void check(String name, String endpoint, AbstractRequest request, Errors expected) throws Exception {
        var response = ProtocolScenarios.exchange(endpoint, request);
        System.out.println("RESPONSE\t" + name + "\t" + response.data());
        Errors actual = Errors.forCode(response instanceof ControllerRegistrationResponse registration ? registration.data().errorCode() : ((UnregisterControllerResponse) response).data().errorCode());
        if (actual != expected) {
            throw new IllegalStateException(name + ": expected " + expected + " got " + response.errorCounts());
        }
        System.out.println("SCENARIO\t" + name + "\t" + expected.name());
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !(args[1].equals("prepare") || args[1].equals("recover"))) {
            throw new IllegalArgumentException("isolated controller endpoint and prepare/recover required");
        }
        var remove = new UnregisterControllerRequest(new UnregisterControllerRequestData().setControllerId(424243), (short) 0);
        if (args[1].equals("prepare")) {
            var features = new ControllerRegistrationRequestData.FeatureCollection();
            features.add(new ControllerRegistrationRequestData.Feature().setName("metadata.version").setMinSupportedVersion((short) 1).setMaxSupportedVersion((short) 33));
            features.add(new ControllerRegistrationRequestData.Feature().setName("kraft.version").setMinSupportedVersion((short) 0).setMaxSupportedVersion((short) 1));
            var listeners = new ControllerRegistrationRequestData.ListenerCollection();
            listeners.add(new ControllerRegistrationRequestData.Listener().setName("CONTROLLER").setHost("localhost").setPort(65529).setSecurityProtocol((short) 0));
            check("register-before-restart", args[0], new ControllerRegistrationRequest(new ControllerRegistrationRequestData()
                .setControllerId(424243).setIncarnationId(new Uuid(4, 424243)).setFeatures(features).setListeners(listeners), (short) 0), Errors.NONE);
            check("remove-before-restart", args[0], remove, Errors.NONE);
        }
        check(args[1].equals("prepare") ? "removed-before-restart" : "removed-after-restart", args[0], remove, Errors.CONTROLLER_ID_NOT_REGISTERED);
    }
}
