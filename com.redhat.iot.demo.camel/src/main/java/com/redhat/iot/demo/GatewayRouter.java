package com.redhat.iot.demo;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.kura.camel.cloud.KuraCloudComponent;
import org.eclipse.kura.camel.component.Configuration;
import org.eclipse.kura.camel.runner.CamelRunner;
import org.eclipse.kura.camel.runner.ServiceConsumer;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.example.ble.tisensortag.BluetoothLe;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.eclipse.kura.camel.component.Configuration.asInt;

/**
 * Example of the Kura Camel application.
 */
public class GatewayRouter implements ConfigurableComponent {

    private static final Logger logger = LoggerFactory.getLogger(GatewayRouter.class);
    private BluetoothLe ble;
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * A RouterBuilder instance which has no routes
     */
    private static final RouteBuilder NO_ROUTES = new RouteBuilder() {

        @Override
        public void configure() throws Exception {
        }
    };

    private CamelRunner camel;

    private String cloudServiceFilter;

    public void start(final Map<String, Object> properties) throws Exception {
        logger.info("Start: {}", properties);

        // create new filter and instance

        final String cloudServiceFilter = makeCloudServiceFilter(properties);
        this.camel = createCamelRunner(cloudServiceFilter);

        // set routes

        this.camel.setRoutes(fromProperties(properties));

        // start

        this.camel.start();
    }

    public void updated(final Map<String, Object> properties) throws Exception {
        logger.info("Updating: {}", properties);

        final String cloudServiceFilter = makeCloudServiceFilter(properties);
        if (!this.cloudServiceFilter.equals(cloudServiceFilter)) {
            // update the routes and the filter

            // stop the camel context first
            this.camel.stop();

            // create a new camel runner, with new dependencies
            this.camel = createCamelRunner(cloudServiceFilter);

            // set the routes
            this.camel.setRoutes(fromProperties(properties));

            // and restart again
            this.camel.start();
        } else {
            // only update the routes, this is done without restarting the context

            this.camel.setRoutes(fromProperties(properties));
        }
    }

    public void stop() throws Exception {
        if (this.camel != null) {
            this.camel.stop();
            this.camel = null;
        }
    }

    private CamelRunner createCamelRunner(final String fullFilter) throws InvalidSyntaxException {
        final BundleContext ctx = FrameworkUtil.getBundle(GatewayRouter.class).getBundleContext();

        this.cloudServiceFilter = fullFilter;

        // create a new camel CamelRunner.Builder

        final CamelRunner.Builder builder = new CamelRunner.Builder();

        // add service dependency

        builder.dependOn(ctx, FrameworkUtil.createFilter(fullFilter),
                new ServiceConsumer<CloudService, CamelContext>() {

                    @Override
                    public void consume(final CamelContext context, final CloudService service) {
                        context.addComponent("cloud", new KuraCloudComponent(context, service));
                    }
                });

        // return un-started instance

        return builder.build();
    }

    /**
     * Construct an OSGi filter for a cloud service instance
     *
     * @param properties
     *            the properties to read from
     * @return the OSGi filter selecting the cloud service instance
     */
    private static String makeCloudServiceFilter(final Map<String, Object> properties) {
        final String filterPid = Configuration.asStringNotEmpty(properties, "cloudService",
                "org.eclipse.kura.cloud.CloudService");
        final String fullFilter = String.format("(&(%s=%s)(kura.service.pid=%s))", Constants.OBJECTCLASS,
                CloudService.class.getName(), filterPid);
        return fullFilter;
    }


    public void setBluetoothLe(BluetoothLe ble) {
      this.ble = ble;
   }

   public void unsetBluetoothLe(BluetoothLe ble) {
      this.ble = null;
   }

    /**
     * Create a new RouteBuilder instance from the properties
     *
     * @param properties
     *            the properties to read from
     * @return the new instance of RouteBuilder
     */
    protected RouteBuilder fromProperties(final Map<String, Object> properties) {

        if (!Configuration.asBoolean(properties, "enabled")) {
            return NO_ROUTES;
        }

        final int maxTemp = asInt(properties, "temperature.max", 20);

        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
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
//                        .log("Publishing Metrics (Outer) =>")
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
//                        .log("Publishing Metrics (Inner) =>")
                        .wireTap("log:WireTap?showAll=true&multiline=true")
                        .wireTap("direct:checkSensors")
                        .toD("cloud:demo-kit/assets/${exchangeProperty.deviceId}")
                        .end();

                // Subscribe to topics on Everyware Cloud
                //      from("cloud:demo-kit/assets/").
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
                        .log("Low Light Event for ${exchangeProperty.deviceId}")
                        .toD("cloud:demo-kit/notification/${exchangeProperty.deviceId}")
                        //               .to("log:LowLightWarning") //?showAll=true&multiline=true")
                        .when(simple("${body.metrics()[Light]} > 275"))
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
                        .log("High Light Event for ${exchangeProperty.deviceId}")
                        .toD("cloud:demo-kit/notification/${exchangeProperty.deviceId}")
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
                        .log("Target Light Event for ${exchangeProperty.deviceId}")
                        .toD("cloud:demo-kit/notification/${exchangeProperty.deviceId}");
                //               .to("log:TargetLight");

/*
      from("cloud:demo-kit/notification/#")
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






/*
                from("timer://heartbeat").process(new Processor() {

                    @Override
                    public void process(Exchange exchange) throws Exception {
                        KuraPayload payload = new KuraPayload();
                        payload.addMetric("temperature", new Random().nextInt(maxTemp));
                        exchange.getIn().setBody(payload);
                    }
                }).to("cloud:myapp/topic").id("temp-heartbeat");

                from("cloud:myapp/topic") //
                        .choice() //
                        .when(simple("${body.metrics()[temperature]} < 10")).to("log:lessThanTen") //
                        .when(simple("${body.metrics()[temperature]} == 10")).to("log:equalToTen") //
                        .otherwise().to("log:greaterThanTen") //
                        .id("test-log");

                from("timer://xmltopic").process(new Processor() {

                    @Override
                    public void process(Exchange exchange) throws Exception {
                        KuraPayload payload = new KuraPayload();
                        payload.addMetric("temperature", new Random().nextInt(20));
                        exchange.getIn().setBody(payload);
                    }
                }).to("cloud:myapp/xmltopic");
*/
            }
        };
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