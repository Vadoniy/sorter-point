package ru.otus.sorterpoint.consumer;

import avro.schema.SmartPhoneAvro;
import avro.schema.enums.ManufacturerAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class SorterPointService {
    private static final String BATTERY_CHANGE_POSTFIX = "-battery-change";
    private static final String MOTHER_BOARD_CHANGE_POSTFIX = "-mother-board-change";
    private static final String SCREEN_CHANGE_POSTFIX = "-screen-change";
    private static final String WORKING_PREFIX = "working-";
    private static final String RECOVERY_PREFIX = "recovery-";
    private static final String DISPOSAL = "disposal-phones";
    private static final String TOPIC_POSTFIX = "-topic";
    public static final String RECEPTION_POINT_TOPIC = "reception-point-topic";

    @Bean
    public KStream<UUID, SmartPhoneAvro> sortingKStream(StreamsBuilder kStreamBuilder) {
        final var consumerSerde = new SpecificAvroSerde<SmartPhoneAvro>();
        final var serdeConfig = Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerSerde.configure(serdeConfig, false);
        final var stream = kStreamBuilder.stream(RECEPTION_POINT_TOPIC,
                Consumed.with(Serdes.UUID(), consumerSerde));
        final var splitterByManufacturer = "split-by-manufacturer-";
        final var phonesByManufacturer = stream
                .peek((key, p) -> log.info("Received phone with\nmodel:{}\nssn: {}\nis broken:{}\nmother board state:{}\nscreen state:{}",
                        p.getModel(), p.getSsn(), p.getMotherBoard().getBroken() || p.getScreen().getBroken(), p.getMotherBoard().getBroken(), p.getScreen().getBroken()))
                .split(Named.as(splitterByManufacturer))
                .branch((ssn, phone) -> phone.getManufacturer().name().endsWith(ManufacturerAvro.APPLE.name()), Branched.as(ManufacturerAvro.APPLE.name()))
                .branch((ssn, phone) -> phone.getManufacturer().name().endsWith(ManufacturerAvro.SAMSUNG.name()), Branched.as(ManufacturerAvro.SAMSUNG.name()))
                .branch((ssn, phone) -> phone.getManufacturer().name().endsWith(ManufacturerAvro.XIAOMI.name()), Branched.as(ManufacturerAvro.XIAOMI.name()))
                .noDefaultBranch();
        final var applePhones = phonesByManufacturer.get(splitterByManufacturer + ManufacturerAvro.APPLE.name());
        final var samsungPhones = phonesByManufacturer.get(splitterByManufacturer + ManufacturerAvro.SAMSUNG.name());
        final var xiaomiPhones = phonesByManufacturer.get(splitterByManufacturer + ManufacturerAvro.XIAOMI.name());

        processBrokenPhones(applePhones, ManufacturerAvro.APPLE.name());
        processBrokenPhones(samsungPhones, ManufacturerAvro.SAMSUNG.name());
        processBrokenPhones(xiaomiPhones, ManufacturerAvro.XIAOMI.name());

        return stream;
    }

    private void processBrokenPhones(KStream<UUID, SmartPhoneAvro> smartPhones, String manufacturerName) {
        final var splitterByCondition = "split-by-condition-" + manufacturerName + "-";
        final var splitBranches = smartPhones
                .split(Named.as(splitterByCondition))
                .branch((ssn, phone) -> !phone.getMotherBoard().getBroken()
                        && !phone.getScreen().getBroken(), Branched.as(WORKING_PREFIX))
                .branch((ssn, phone) -> phone.getMotherBoard().getBroken()
                        && phone.getScreen().getBroken(), Branched.as(DISPOSAL))
                .defaultBranch(Branched.as(RECOVERY_PREFIX));
        splitBranches.get(splitterByCondition + WORKING_PREFIX).to(manufacturerName + BATTERY_CHANGE_POSTFIX + TOPIC_POSTFIX);
        splitBranches.get(splitterByCondition + DISPOSAL).to(DISPOSAL + TOPIC_POSTFIX);

        final var splitterByBrokenDetail = "split-by-broken-detail-" + manufacturerName + "-";
        final var brokenPhonesBranches = splitBranches.get(splitterByCondition + RECOVERY_PREFIX)
                .split(Named.as(splitterByBrokenDetail))
                .branch((ssn, phone) -> phone.getMotherBoard().getBroken(), Branched.as(MOTHER_BOARD_CHANGE_POSTFIX))
                .defaultBranch(Branched.as(SCREEN_CHANGE_POSTFIX));
        brokenPhonesBranches.get(splitterByBrokenDetail + MOTHER_BOARD_CHANGE_POSTFIX).to(manufacturerName + MOTHER_BOARD_CHANGE_POSTFIX + TOPIC_POSTFIX);
        brokenPhonesBranches.get(splitterByBrokenDetail + SCREEN_CHANGE_POSTFIX).to(manufacturerName + SCREEN_CHANGE_POSTFIX + TOPIC_POSTFIX);
    }
}
