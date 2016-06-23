package io.rhiot.quickstarts.kura.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.kura.camel.cloud.KuraCloudComponent;
import org.eclipse.kura.camel.router.CamelRouter;
import org.eclipse.kura.example.ble.tisensortag.BluetoothLe;
import org.eclipse.kura.message.KuraPayload;

import java.util.Map;

/**
 * Example of the Kura Camel application.
 */
public class GatewayRouter extends CamelRouter {

   private BluetoothLe ble;
   final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();


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

      // Poll interval to check for pending sensor payloads and publish to Everyware Cloud
      from("timer://heartbeat?fixedRate=true&period=5000")
           .onCompletion()
              .process(new Processor() {
                 @Override
                 public void process(Exchange exchange) throws Exception {
                    ble.clearKuraPayloads();
                 }
              })
           .end()
           .process(new Processor() {
              @Override
              public void process(Exchange exchange) throws Exception {
                 if (ble.getKuraPayloads() != null && ble.getKuraPayloads().size() > 0) {
                    exchange.setProperty("kurapayloads", ble.getKuraPayloads());
                 }
              }
           })
//           .log("Publishing Metrics (Outer) =>")
           .split(simple("${exchangeProperty.kurapayloads.keySet}"))
              .parallelProcessing()
              .process(new Processor() {
                 @Override
                 public void process(Exchange exchange) throws Exception {
                    String deviceId = (String) exchange.getIn().getBody();
                    Map<String, KuraPayload> map = (Map<String, KuraPayload>) exchange.getProperty("kurapayloads");
                    exchange.setProperty("deviceId", deviceId);
                    exchange.getIn().setBody(map.get(deviceId));
                 }
              })
//              .log("Publishing Metrics (Inner) =>")
//              .wireTap("log:WireTap?showAll=true&multiline=true")
              .wireTap("direct:checkSensors")
              .toD("kura-cloud:summit-demo/assets/${exchangeProperty.deviceId}")
           .end();

      // Subscribe to topics on Everyware Cloud
//      from("kura-cloud:summit-demo/assets/").
      from("direct:checkSensors").
              choice().
              when(simple("${body.metrics()[Light]} < 100"))
               .process(new Processor() {
                 @Override
                 public void process(Exchange exchange) throws Exception {
                    String deviceAddress = (String)exchange.getProperty("deviceId"); //getDeviceAddressFromTopic((String) exchange.getIn().getHeader("CamelKuraCloudService.topic"));
                    KuraPayload payload = new KuraPayload();
                    payload.addMetric("red", false);
                    payload.addMetric("green", false);
                    exchange.getIn().setBody(payload);
                    ble.switchOffRedLed(deviceAddress);
                    ble.switchOffGreenLed(deviceAddress);
                 }
               })
//               .log("Low Light Event for ${exchangeProperty.deviceId}")
               .toD("kura-cloud:summit-demo/notification/${exchangeProperty.deviceId}")
//               .to("log:LowLightWarning") //?showAll=true&multiline=true")
              .when(simple("${body.metrics()[Light]} > 150"))
               .process(new Processor() {
                 @Override
                 public void process(Exchange exchange) throws Exception {
                    String deviceAddress = (String)exchange.getProperty("deviceId"); //getDeviceAddressFromTopic((String) exchange.getIn().getHeader("CamelKuraCloudService.topic"));
                    KuraPayload payload = new KuraPayload();
                    payload.addMetric("red", true);
                    payload.addMetric("green", false);
                    exchange.getIn().setBody(payload);
                    ble.switchOnRedLed(deviceAddress);
                    ble.switchOffGreenLed(deviceAddress);
                 }
               })
//               .log("High Light Event for ${exchangeProperty.deviceId}")
               .toD("kura-cloud:summit-demo/notification/${exchangeProperty.deviceId}")
//               .to("log:HighLightWarning?showAll=true&multiline=true")
              .otherwise()
               .process(new Processor() {
                  @Override
                  public void process(Exchange exchange) throws Exception {
                     String deviceAddress = (String)exchange.getProperty("deviceId"); //getDeviceAddressFromTopic((String) exchange.getIn().getHeader("CamelKuraCloudService.topic"));
                     KuraPayload payload = new KuraPayload();
                     payload.addMetric("red", false);
                     payload.addMetric("green", true);
                     exchange.getIn().setBody(payload);
                     ble.switchOffRedLed(deviceAddress);
                     ble.switchOnGreenLed(deviceAddress);
                  }
               })
//               .log("Target Light Event for ${exchangeProperty.deviceId}")
               .toD("kura-cloud:summit-demo/notification/${exchangeProperty.deviceId}");
//               .to("log:TargetLight");

/*
      from("kura-cloud:summit-demo/notification/#")
            .process(new Processor() {
               @Override
               public void process(Exchange exchange) throws Exception {
                  byte[] body = ((KuraPayload)exchange.getIn().getBody()).getBody();
                  log.info("Exchange is: " + ((KuraPayload)exchange.getIn().getBody()).getBody());
                  char[] hexChars = new char[body.length * 2];
                  for ( int j = 0; j < body.length; j++ ) {
                     int v = body[j] & 0xFF;
                     hexChars[j * 2] = hexArray[v >>> 4];
                     hexChars[j * 2 + 1] = hexArray[v & 0x0F];
                  }
                  log.info(new String(hexChars));
                  exchange.getIn().setBody(new String(hexChars));
*/
/*
                  log.info(bytesToHex(body));
                  exchange.getIn().setBody(bytesToHex(body));
*//*

               }
            })
            .log("Expression value for body: ${body}")
         .to("log:Notification?showAll=true&multiline=true");
*/

   }

   private static String getDeviceAddressFromTopic(String in) {
      return in.substring(in.lastIndexOf("/") + 1);
   }

   private static String bytesToHex(byte[] in) {
      final StringBuilder builder = new StringBuilder();
      for(byte b : in) {
         builder.append(String.format("%02x", b));
      }
      return builder.toString();
   }

}