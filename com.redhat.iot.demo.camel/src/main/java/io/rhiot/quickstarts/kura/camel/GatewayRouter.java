package io.rhiot.quickstarts.kura.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.kura.camel.camelcloud.DefaultCamelCloudService;
import org.eclipse.kura.camel.cloud.KuraCloudComponent;
import org.eclipse.kura.camel.router.CamelRouter;
import org.eclipse.kura.message.KuraPayload;

import java.util.Random;

/**
 * Example of the Kura Camel application.
 */
public class GatewayRouter extends CamelRouter {

    @Override
    public void configure() throws Exception {
        KuraCloudComponent cloudComponent = new KuraCloudComponent();
        cloudComponent.setCamelContext(camelContext);
        cloudComponent.getCloudService();
        camelContext.addComponent("kura-cloud", cloudComponent);

        from("timer://heartbeat?fixedRate=true&period=10000").
                process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        KuraPayload payload = new KuraPayload();
                        payload.addMetric("temperature", new Random().nextInt(20));
                        exchange.getIn().setBody(payload);
                    }
                }).to("kura-cloud:myapp/topic");

//        from("kura-cloud:summit-demo/assets/#").
        from("kura-cloud:summit-demo/assets/B0:B4:48:BC:D2:85").
                choice().
                  when(simple("${body.metrics()[Ambient]} < 29"))
                .to("log:LowTempWarning")
                  .when(simple("${body.metrics()[Ambient]} == 29"))
                  .to("log:TargetTemp")
                .otherwise()
                .to("log:HighTempWarning");

    }

}