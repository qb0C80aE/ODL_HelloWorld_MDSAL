package helloworld;

import org.opendaylight.controller.sal.binding.api.AbstractBindingAwareConsumer;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.controller.sal.binding.api.data.DataBrokerService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yangtools.concepts.Registration;
import org.opendaylight.yangtools.yang.binding.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Activator extends AbstractBindingAwareConsumer implements
        AutoCloseable {

    /** Logger **/
    private static Logger logger = LoggerFactory
            .getLogger(Activator.class);

    /** リスナ **/
    private Registration<NotificationListener> listenerRegistration;

    /**
     * 開始処理
     */
    @Override
    public void onSessionInitialized(
            BindingAwareBroker.ConsumerContext consumerContext) {

        logger.info("onSessionInitialized is called");

        // DataBrokerService: binding API、データストアに統一的にアクセスする手段を提供するAPI
        // モデルの変更通知を受け取るためにも利用
        DataBrokerService dataBrokerService = consumerContext
                .<DataBrokerService> getSALService(DataBrokerService.class);

        // NotificationService: binding API、SALの通知サービス
        // Packet-Inなどの通知を受けるために利用
        NotificationService notificationService = consumerContext
                .<NotificationService> getSALService(NotificationService.class);

        // PacketProcessingService: RPCのAPI。OpenFlowPlugin側のIF
        // SBとはRPCでやりとりする
        PacketProcessingService packetProcessingService = consumerContext
                .<PacketProcessingService> getRpcService(PacketProcessingService.class);
        HelloWorld helloworld = new HelloWorld();

        // 利用サービスの設定
        helloworld.setPacketProcessingService(packetProcessingService);
        helloworld.setDataBrokerService(dataBrokerService);

        // MD-SALにリスナを登録し、コールバックを受信できるようにする
        this.listenerRegistration = notificationService
                .registerNotificationListener(helloworld);

    }

    /**
     * 終了処理
     */
    @Override
    public void close() throws Exception {
        logger.info("close is called");
        if (listenerRegistration != null) {
            listenerRegistration.close();
            listenerRegistration = null;
        }
    }
}
