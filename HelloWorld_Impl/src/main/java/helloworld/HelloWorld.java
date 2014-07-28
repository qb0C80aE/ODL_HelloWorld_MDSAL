package helloworld;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.opendaylight.controller.sal.binding.api.data.DataBrokerService;
import org.opendaylight.controller.sal.binding.api.data.DataModificationTransaction;
import org.opendaylight.controller.sal.packet.Ethernet;
import org.opendaylight.controller.sal.packet.LinkEncap;
import org.opendaylight.controller.sal.packet.Packet;
import org.opendaylight.controller.sal.packet.RawPacket;
import org.opendaylight.controller.sal.utils.EtherTypes;
import org.opendaylight.controller.sal.utils.HexEncode;
import org.opendaylight.controller.sal.utils.NetUtils;
import org.opendaylight.yang.gen.v1.urn.helloworld.macport.rev140726.MacPortEntries;
import org.opendaylight.yang.gen.v1.urn.helloworld.macport.rev140726.mac.port.entries.MacPortEntry;
import org.opendaylight.yang.gen.v1.urn.helloworld.macport.rev140726.mac.port.entries.MacPortEntryBuilder;
import org.opendaylight.yang.gen.v1.urn.helloworld.macport.rev140726.mac.port.entries.MacPortEntryKey;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.OutputActionCase;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.OutputActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.output.action._case.OutputAction;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.output.action._case.OutputActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.Action;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.ActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowModFlags;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.Instructions;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.InstructionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.Match;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.MatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.ApplyActionsCase;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.ApplyActionsCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActions;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.Instruction;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.InstructionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetDestination;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetDestinationBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetSource;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetSourceBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatch;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInputBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorld implements PacketProcessingListener {

    /** Logger **/
    private final static Logger log = LoggerFactory.getLogger(HelloWorld.class);

    /** ブロードキャストアドレス */
    private final static String MAC_ADDRESS_BROADCAST = "ff:ff:ff:ff:ff:ff";

    /** PacketProcessingService **/
    private PacketProcessingService packetProcessingService;

    /** DataBrokerService **/
    private DataBrokerService dataBrokerService;

    /** FlowIDのカウンタ */
    private AtomicLong flowIdCounter = new AtomicLong();

    /**
     * PacketProcessingServiceの設定
     * 
     * @param packetProcessingService
     *            PacketProcessingServiceのインスタンス
     */
    public void setPacketProcessingService(
            PacketProcessingService packetProcessingService) {
        this.packetProcessingService = packetProcessingService;
    }

    /**
     * DataBrokerServiceの設定
     * 
     * @param dataBrokerService
     *            DataBrokerServiceのインスタンス
     */
    public void setDataBrokerService(DataBrokerService dataBrokerService) {
        this.dataBrokerService = dataBrokerService;
    }

    /**
     * MACアドレスとPortの変換エントリを取得
     * 
     * @param macAddress
     *            取得するエントリのキー(MACアドレス)
     * @param nodeConnectorRef
     *            取得するエントリのキー(Node)を持つPort
     * @return MACアドレスとPortの変換エントリ
     */
    private MacPortEntry getMacPortEntry(MacAddress macAddress,
            NodeConnectorRef nodeConnectorRef) {
        // MD-SALのデータストアから読み込み
        return (MacPortEntry) (dataBrokerService
                .readOperationalData(createMacPortEntryInstanceIdentifier(
                        macAddress, nodeConnectorRef)));
    }

    /**
     * PACKET-IN処理
     * 
     * @param packetReceived
     *            受信したパケット
     */
    @Override
    public void onPacketReceived(PacketReceived packetReceived) {

        try {
            // ペイロード取得、形式の変換
            byte[] payload = packetReceived.getPayload();
            RawPacket rawPacket = new RawPacket(payload);
            Packet packet = decodePacket(rawPacket);

            NodeConnectorRef ingressNodeConnectorRef = packetReceived
                    .getIngress();

            // Ethernetフレーム以外は無視
            if (!(packet instanceof Ethernet)) {
                return;
            }

            // 入力元、出力先MACアドレスを取得
            Ethernet ethernetFrame = (Ethernet) (packet);
            byte[] sourceMacAddressByteArray = ethernetFrame
                    .getSourceMACAddress();
            byte[] destinationMacAddressByteArray = ethernetFrame
                    .getDestinationMACAddress();

            if ((sourceMacAddressByteArray == null)
                    || (sourceMacAddressByteArray.length == 0)) {
                log.debug("invalid sourec MAC address.");
                return;
            }

            // LLDPは無視
            if (ethernetFrame.getEtherType() == EtherTypes.LLDP.shortValue()) {
                log.debug("inPkt is a LLDP Packet.");
                return;
            }

            // IPv6は無視
            if (ethernetFrame.getEtherType() == EtherTypes.IPv6.shortValue()) {
                log.debug("inPkt is an IPv6 Packet.");
                return;
            }

            MacAddress sourceMacAddress = new MacAddress(
                    HexEncode.bytesToHexStringFormat(sourceMacAddressByteArray));
            MacAddress destinationMacAddress = new MacAddress(
                    HexEncode
                            .bytesToHexStringFormat(destinationMacAddressByteArray));

            // 入力元Port・スイッチをキーにしてデータストアから取得し、存在しない場合はMACアドレス・スイッチとPortのペアを追加する
            MacPortEntry sourceMacPortEntry = getMacPortEntry(sourceMacAddress,
                    ingressNodeConnectorRef);

            if (sourceMacPortEntry == null) {
                // ない場合は追加（学習）
                log.debug("learn: " + sourceMacAddress.getValue());
                sourceMacPortEntry = learnMac(sourceMacAddress,
                        ingressNodeConnectorRef);
            }

            // ブロードキャストの場合はfloodingする
            log.debug("sourceMacAddress = " + sourceMacAddress.getValue());
            log.debug("destinationMacAddress = "
                    + destinationMacAddress.getValue());

            if (destinationMacAddress.getValue().equals(MAC_ADDRESS_BROADCAST)) {
                log.debug("a broadcast packet is detected.");
                flood(payload, ingressNodeConnectorRef);
                return;
            }

            // 出力先Portをデータストアから取得し、存在する場合はそこへ出力
            MacPortEntry destinationMacPortEntry = getMacPortEntry(
                    destinationMacAddress, ingressNodeConnectorRef);

            // ブロードキャストでもなく出力先が不明な場合は無視
            if (destinationMacPortEntry == null) {
                log.warn("destinationMacPortEntry is null.");
                return;
            }

            // 出力Portの取得
            NodeConnectorRef egressNodeConnectorRef = destinationMacPortEntry
                    .getPort();

            // Flowのモデルを更新（=> FlowMod発生）
            updateFlow(sourceMacAddress, destinationMacAddress,
                    egressNodeConnectorRef);

            // 初期パケットの出力
            forward(payload, ingressNodeConnectorRef, egressNodeConnectorRef);

        } catch (Exception e) {
            log.error("error. ", packetReceived, e);
        }
    }

    /**
     * Flowを更新
     * 
     * @param sourceMacAddress
     *            入力元MACアドレス
     * @param destinationMacAddress
     *            入力先MACアドレス
     * @param destinationNodeConnectorRef
     *            入力先Port
     */
    private void updateFlow(MacAddress sourceMacAddress,
            MacAddress destinationMacAddress,
            NodeConnectorRef egressNodeConnectorRef) {

        // TODO:今回はテーブル0に固定
        Short tableId = 0;
        TableKey tableKey = new TableKey((short) tableId);
        int priority = 0;
        FlowId flowId = new FlowId(String.valueOf(flowIdCounter
                .getAndIncrement()));
        FlowKey flowKey = new FlowKey(flowId);
        String flowName = flowId.getValue();

        // Flowのパスを作成
        // まずNodeのパス
        InstanceIdentifier<Node> nodeInstanceIdentifier = extractNodeInstanceIdentifier(egressNodeConnectorRef
                .getValue());

        log.debug("nodeInstanceIdentifier = "
                + nodeInstanceIdentifier.toString());

        // Node->Table(0)のパス
        InstanceIdentifier<Table> tableInstanceIdentifier = InstanceIdentifier
                .<Node> builder(nodeInstanceIdentifier)
                .augmentation(FlowCapableNode.class)
                .child(Table.class, tableKey).build();

        log.debug("tableInstanceIdentifier = "
                + tableInstanceIdentifier.toString());

        // Node->Table(0)->Flow(flowIdCounter)のパス
        InstanceIdentifier<Flow> flowInstanceIdentifier = InstanceIdentifier
                .<Table> builder(tableInstanceIdentifier)
                .<Flow, FlowKey> child(Flow.class, flowKey).build();

        log.debug("flowInstanceIdentifier = "
                + flowInstanceIdentifier.toString());

        // Matchの作成
        EthernetDestinationBuilder ethernetDestinationBuilder = new EthernetDestinationBuilder();
        ethernetDestinationBuilder.setAddress(destinationMacAddress);
        EthernetDestination ethernetDestination = ethernetDestinationBuilder
                .build();

        EthernetSourceBuilder ethernetSourceBuilder = new EthernetSourceBuilder();
        ethernetSourceBuilder.setAddress(sourceMacAddress);
        EthernetSource ethernetSource = ethernetSourceBuilder.build();

        EthernetMatchBuilder ethernetMatchBuilder = new EthernetMatchBuilder();
        ethernetMatchBuilder.setEthernetSource(ethernetSource);
        ethernetMatchBuilder.setEthernetDestination(ethernetDestination);
        EthernetMatch ethernetMatch = ethernetMatchBuilder.build();

        MatchBuilder matchBuilder = new MatchBuilder();
        matchBuilder.setEthernetMatch(ethernetMatch);
        Match match = matchBuilder.build();

        // Actionの作成
        Uri outputPort = egressNodeConnectorRef.getValue()
                .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
                .getId();

        OutputActionBuilder outputActionBuilder = new OutputActionBuilder();
        outputActionBuilder.setMaxLength(new Integer(0xffff));
        outputActionBuilder.setOutputNodeConnector(outputPort);
        OutputAction outputAction = outputActionBuilder.build();

        OutputActionCaseBuilder outputActionCaseBuilder = new OutputActionCaseBuilder();
        outputActionCaseBuilder.setOutputAction(outputAction);
        OutputActionCase outputActionCase = outputActionCaseBuilder.build();

        ActionBuilder actionBuilder = new ActionBuilder();
        actionBuilder.setOrder(0);
        actionBuilder.setAction(outputActionCase);
        Action action = actionBuilder.build();

        List<Action> actionList = new ArrayList<Action>();
        actionList.add(action);

        ApplyActionsBuilder applyActionsBuilder = new ApplyActionsBuilder();
        applyActionsBuilder.setAction(actionList);

        ApplyActions applyActions = applyActionsBuilder.build();

        // Instructionの作成
        ApplyActionsCaseBuilder applyActionsCaseBuilder = new ApplyActionsCaseBuilder();
        applyActionsCaseBuilder.setApplyActions(applyActions);
        ApplyActionsCase applyActionsCase = applyActionsCaseBuilder.build();

        InstructionBuilder instructionBuilder = new InstructionBuilder();
        instructionBuilder.setOrder(0);
        instructionBuilder.setInstruction(applyActionsCase);
        Instruction instruction = instructionBuilder.build();

        List<Instruction> instructionList = new ArrayList<Instruction>();
        instructionList.add(instruction);

        InstructionsBuilder instructionsBuilder = new InstructionsBuilder();
        instructionsBuilder.setInstruction(instructionList);
        Instructions instructions = instructionsBuilder.build();

        // FlowModFlagsの作成
        FlowModFlags flowModFlags = new FlowModFlags(false, false, false,
                false, false);

        // Flowの作成
        FlowBuilder flowBuilder = new FlowBuilder();
        flowBuilder.setTableId(tableId);
        flowBuilder.setFlowName(flowName);
        flowBuilder.setId(new FlowId(Long.toString(flowBuilder.hashCode())));

        flowBuilder.setMatch(match);
        flowBuilder.setInstructions(instructions);
        flowBuilder.setPriority(priority);
        flowBuilder.setBufferId(0L);
        flowBuilder.setHardTimeout(0);
        flowBuilder.setIdleTimeout(0);
        flowBuilder.setFlags(flowModFlags);
        // TODO: 今回は0固定
        flowBuilder.setCookie(new BigInteger("0"));

        Flow flow = flowBuilder.build();

        // MD-SALのデータストアのFlowを更新
        DataModificationTransaction addFlowTransaction = dataBrokerService
                .beginTransaction();
        addFlowTransaction.putConfigurationData(flowInstanceIdentifier, flow);
        addFlowTransaction.commit();

    }

    /**
     * 入力Port以外にfloodingする
     * 
     * @param payload
     *            ペイロード
     * @param ingressNodeConnectorRef
     *            入力元Port
     */
    private void flood(byte[] payload, NodeConnectorRef ingressNodeConnectorRef) {
        log.debug("flood.");

        // 入力元Port以外のPortに出力
        InstanceIdentifier<Node> nodeInstanceIdentifier = extractNodeInstanceIdentifier(ingressNodeConnectorRef
                .getValue());
        Node node = (Node) (dataBrokerService
                .readOperationalData(nodeInstanceIdentifier));

        NodeConnectorKey ingressNodeConnectorKey = ingressNodeConnectorRef
                .getValue().firstKeyOf(NodeConnector.class,
                        NodeConnectorKey.class);
        NodeConnectorId ingressNodeConnectorId = ingressNodeConnectorKey
                .getId();

        for (NodeConnector nodeConnector : node.getNodeConnector()) {

            // 入力Portでなければ出力する
            if (!nodeConnector.getId().getValue()
                    .equals(ingressNodeConnectorId.getValue())) {
                log.debug("unmatched. flood --> forward.");

                // 出力Portの取得
                NodeConnectorRef egressNodeConnectorRef = new NodeConnectorRef(
                        InstanceIdentifier
                                .<Nodes> builder(Nodes.class)
                                .<Node, NodeKey> child(Node.class,
                                        node.getKey())
                                .<NodeConnector, NodeConnectorKey> child(
                                        NodeConnector.class,
                                        nodeConnector.getKey()).toInstance());

                forward(payload, ingressNodeConnectorRef,
                        egressNodeConnectorRef);
            } else {
                log.debug("matched. ignore.");
            }
        }

    }

    /**
     * Portに出力
     * 
     * @param payload
     *            ペイロード
     * @param ingressNodeConnectorRef
     *            入力Port
     * @param egressNodeConnectorRef
     *            出力Port
     */
    private void forward(byte[] payload,
            NodeConnectorRef ingressNodeConnectorRef,
            NodeConnectorRef egressNodeConnectorRef) {

        log.debug("forward.");

        // ノードのパス（データストアのツリーノードの位置）
        InstanceIdentifier<Node> egressNodeInstanceIdentifier = extractNodeInstanceIdentifier(egressNodeConnectorRef
                .getValue());

        // 出力パケット生成
        TransmitPacketInputBuilder transmitPacketInputBuilder = new TransmitPacketInputBuilder();
        transmitPacketInputBuilder.setNode(new NodeRef(
                egressNodeInstanceIdentifier));
        transmitPacketInputBuilder.setIngress(ingressNodeConnectorRef);
        transmitPacketInputBuilder.setEgress(egressNodeConnectorRef);
        transmitPacketInputBuilder.setPayload(payload);
        transmitPacketInputBuilder.build();
        TransmitPacketInput outPkt = transmitPacketInputBuilder.build();

        // パケット出力
        packetProcessingService.transmitPacket(outPkt);

    }

    /**
     * ペイロードのデコード処理
     * 
     * @param rawPacket
     *            The raw packet to be decoded.
     * @return デコードされたパケット
     */
    private Packet decodePacket(RawPacket rawPacket) {
        byte[] packetData = rawPacket.getPacketData();
        // Ethernetフレームの場合のみ処理する
        if (rawPacket.getEncap().equals(LinkEncap.ETHERNET)) {
            Ethernet ethernetFrame = new Ethernet();
            try {
                ethernetFrame.deserialize(packetData, 0, packetData.length
                        * NetUtils.NumBitsInAByte);
                ethernetFrame.setRawPayload(rawPacket.getPacketData());
            } catch (Exception e) {
                log.warn("invalid packet.", e);
                return null;
            }
            return ethernetFrame;
        }
        return null;
    }

    /**
     * MACアドレスの学習
     * 
     * @param sourceMacAddress
     *            入力MACアドレス
     * @param ingressNodeConnectorRef
     *            入力Port
     * @param node
     *            対象スイッチ
     * @return 対象スイッチのMACアドレステーブル
     */
    private MacPortEntry learnMac(MacAddress sourceMacAddress,
            NodeConnectorRef ingressNodeConnectorRef) {

        InstanceIdentifier<Node> ingressNodeInstanceIdentifier = extractNodeInstanceIdentifier(ingressNodeConnectorRef
                .getValue());

        // MacアドレスとPortの対応エントリを作成
        final MacPortEntryBuilder builder = new MacPortEntryBuilder();
        builder.setKey(new MacPortEntryKey(sourceMacAddress, new NodeRef(
                ingressNodeInstanceIdentifier)));
        builder.setMacAddress(sourceMacAddress);
        builder.setPort(ingressNodeConnectorRef);
        // 自身を返却してくるので、メソッドチェーンで記述することもできる

        // MD-SALのデータストアにエントリを追加
        MacPortEntry macPortEntry = builder.build();
        DataModificationTransaction transaction = dataBrokerService
                .beginTransaction();
        transaction.putOperationalData(
                createMacPortEntryInstanceIdentifier(sourceMacAddress,
                        ingressNodeConnectorRef), macPortEntry);
        transaction.commit();

        return macPortEntry;

    }

    /**
     * MD-SALのデータストア内のMacPortEntryのInstanceIdentifierを作成する
     * 
     * @param macAddress
     *            MACアドレス(キー)
     * @param nodeConnectorRef
     *            入力Port
     * @return InstanceIdentifier
     */
    private InstanceIdentifier<MacPortEntry> createMacPortEntryInstanceIdentifier(
            MacAddress macAddress, NodeConnectorRef nodeConnectorRef) {

        InstanceIdentifier<Node> nodeInstanceIdentifier = extractNodeInstanceIdentifier(nodeConnectorRef
                .getValue());

        InstanceIdentifier<MacPortEntry> macPortEntryInstanceIdentifier = InstanceIdentifier
                .<MacPortEntries> builder(MacPortEntries.class)
                .<MacPortEntry, MacPortEntryKey> child(
                        MacPortEntry.class,
                        new MacPortEntryKey(macAddress, new NodeRef(
                                nodeInstanceIdentifier))).toInstance();

        return macPortEntryInstanceIdentifier;

    }

    /**
     * Nodeの子(ChildOf<Node>)
     * のReferenceのInstanceIdentifierからNodeのInstanceIdentifierを取得
     * 
     * @param childOfNode
     *            Nodeの子のReferenceのInstanceIdentifier
     * @return
     */
    private InstanceIdentifier<Node> extractNodeInstanceIdentifier(
            InstanceIdentifier<?> childOfNode) {
        return childOfNode.firstIdentifierOf(Node.class);
    }

}
