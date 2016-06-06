package io.rhiot.quickstarts.kura.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.eclipse.kura.camel.camelcloud.DefaultCamelCloudService;
import org.eclipse.kura.camel.cloud.KuraCloudComponent;
import org.eclipse.kura.camel.router.CamelRouter;
import org.eclipse.kura.example.ble.tisensortag.BluetoothLe;
import org.eclipse.kura.message.KuraPayload;

import java.util.Map;
import java.util.Random;

import static org.apache.camel.builder.PredicateBuilder.isNotNull;

/**
 * Example of the Kura Camel application.
 */
public class GatewayRouter extends CamelRouter {

   private BluetoothLe ble;

   public void setBluetoothLe(BluetoothLe ble) {
      this.ble = ble;
   }

   public void unsetBluetoothLe(BluetoothLe ble) {
      this.ble = null;
   }

   @Override
    public void configure() throws Exception {
        KuraCloudComponent cloudComponent = new KuraCloudComponent();
        cloudComponent.setCamelContext(camelContext);
        cloudComponent.getCloudService();
        camelContext.addComponent("kura-cloud", cloudComponent);

        from("timer://heartbeat?fixedRate=true&period=2000").
                process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
//                        KuraPayload payload = new KuraPayload();
//                        payload.addMetric("temperature", new Random().nextInt(20));
                       if (ble.getKuraPayloads() != null && ble.getKuraPayloads().size() > 0) {
                          exchange.getIn().setHeader("kurapayloads", ble.getKuraPayloads());
                          exchange.getIn().setHeader("kurakeys", ble.getKuraPayloads().keySet());

//                          exchange.getIn().setBody(ble.getKuraPayloads().keySet());
                       }
                    }
                }).
                        split(simple("${header.kurakeys}")).
                           log("Body is " + bodyAs(String.class).toString()).
                             process(new Processor() {
                                @Override
                                public void process(Exchange exchange) throws Exception {
                                   String deviceId = (String) exchange.getIn().getBody();
                                   log.warn("Device ID is: " + deviceId);
                                   Map<String, KuraPayload> map = (Map<String, KuraPayload>) exchange.getIn().getHeader("kurapayloads");
                                   log.warn("Map is: " + map);
                                   KuraPayload payload = map.get(deviceId);
                                   exchange.getIn().setHeader("deviceId", deviceId);
                                   exchange.getIn().setBody(payload);
                                }
                             }).
                           log("Processed Message for ${header.deviceId} :" + body()).
                           toD("kura-cloud:summit-demo/assets/${header.deviceId}"); //.


//      .to("mqtt:kura?host=tcp://broker-sandbox.everyware-cloud.com:1883&publishTopicName=Red-Hat/my/topic&userName=c-custine&password=af3!jWx4azmHA!");

//        from("kura-cloud:summit-demo/assets/#").
/*
        from("kura-cloud:summit-demo/assets/B0:B4:48:BC:D2:85").
                choicew().
                  when(simple("${body.metrics()[Ambient]} < 29"))
                .to("log:LowTempWarning")
                  .when(simple("${body.metrics()[Ambient]} == 29"))
                  .to("log:TargetTemp")
                .otherwise()
                .to("log:HighTempWarning");
*/

    }

}