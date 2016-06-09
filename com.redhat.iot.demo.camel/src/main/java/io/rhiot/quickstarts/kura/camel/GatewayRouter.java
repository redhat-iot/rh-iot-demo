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

        from("timer://heartbeat?fixedRate=true&period=10000")
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
              .toD("kura-cloud:summit-demo/assets/${exchangeProperty.deviceId}")
           .end();

      from("kura-cloud:summit-demo/assets/#").
              choice().
              when(simple("${body.metrics()[Light]} < 100"))
               .process(new Processor() {
                 @Override
                 public void process(Exchange exchange) throws Exception {
                    String deviceAddress = getDeviceAddressFromTopic((String) exchange.getIn().getHeader("CamelKuraCloudService.topic"));
                    ble.switchOffRedLed(deviceAddress);
                    ble.switchOffGreenLed(deviceAddress);
//                    ble.switchOffBuzzer(getDeviceAddressFromTopic((String)exchange.getIn().getHeader("CamelKuraCloudService.topic")));
                 }
               })
               .log("Low Light Event for ${header.CamelKuraCloudService.topic}")
               .to("log:LowLightWarning") //?showAll=true&multiline=true")
              .when(simple("${body.metrics()[Light]} > 150"))
               .process(new Processor() {
                 @Override
                 public void process(Exchange exchange) throws Exception {
                    String deviceAddress = getDeviceAddressFromTopic((String) exchange.getIn().getHeader("CamelKuraCloudService.topic"));
                    ble.switchOnRedLed(deviceAddress);
                    ble.switchOnGreenLed(deviceAddress);
//                    ble.switchOnBuzzer(getDeviceAddressFromTopic((String)exchange.getIn().getHeader("CamelKuraCloudService.topic")));
                 }
               })
               .log("Target Light Event for ${header.CamelKuraCloudService.topic}")
               .to("log:TargetTemp")
              .otherwise()
               .process(new Processor() {
                  @Override
                  public void process(Exchange exchange) throws Exception {
                     String deviceAddress = getDeviceAddressFromTopic((String) exchange.getIn().getHeader("CamelKuraCloudService.topic"));
                     ble.switchOnRedLed(deviceAddress);
                     ble.switchOffGreenLed(deviceAddress);
//                     ble.switchOffBuzzer(getDeviceAddressFromTopic((String)exchange.getIn().getHeader("CamelKuraCloudService.topic")));
                  }
               })
               .log("High Light Event for ${header.CamelKuraCloudService.topic}")
               .to("log:HighTempWarning"); //?showAll=true&multiline=true");

   }

   private static String getDeviceAddressFromTopic(String in) {
      return in.substring(in.lastIndexOf("/") + 1);
   }

}