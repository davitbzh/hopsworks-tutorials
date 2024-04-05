import statistics
import json
from datetime import datetime, timedelta, timezone

import bytewax.operators.window as win
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSourceMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.operators.window import EventClockConfig, TumblingWindow, SlidingWindow

import hopsworks
from hsfs_bytewax_util import get_kafka_config, serialize_with_key, sink_kafka


# This is the accumulator function, and outputs a list of 2-tuples,
# containing the event's "value" and it's "time" (used later to print info)
def accumulate(acc, event):
    acc.append(event["amount"])
    return acc


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    date_time = datetime.now()  # datetime.fromisoformat(event["datetime"])
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=timezone.utc)
    return date_time


def format_event(event):
    key, (metadata, data) = event
    values = [x for x in data]

    date_time = datetime.now(timezone.utc)
    date_time = datetime(date_time.year, date_time.month, date_time.day, date_time.hour, date_time.minute,
                         date_time.second)
    date_time = date_time.replace(tzinfo=timezone.utc)

    return key, {
        "cc_num": key,
        "timestamp": date_time,  # int(float(datetime.now(timezone.utc).timestamp()) * 1000),
        "min_amount": min(values),
        "max_amount": max(values),
        "count": len(values),
        "mean": statistics.mean(values)
    }


def get_flow(feature_group_name, feature_group_version, hopsworks_host, hopsworks_project, hopsworks_api_key):
    # connect to hopsworks
    project = hopsworks.login(
        host=hopsworks_host,
        project=hopsworks_project,
        api_key_value=hopsworks_api_key
    )
    fs = project.get_feature_store()

    # get feature group and its topic configuration
    feature_group = fs.get_feature_group(feature_group_name, feature_group_version)

    # get kafka connection config
    kafka_config = get_kafka_config(feature_store_id=fs.id)
    kafka_config["auto.offset.reset"] = "earliest"

    flow = Dataflow("windowing")
    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)

    # This will pass simulated transactions directly to the streaming pipeline
    # inp = get_transactions()
    # parsed_stream = op.input("input", flow, TestingSource(inp))

    # Define the dataflow object and kafka input.
    stream = kop.input(
        "kafka-in", flow, brokers=[kafka_config['bootstrap.servers']], topics=["live_transactions"],
        add_config=kafka_config
    )

    # We expect a json string that represents a reading from a sensor in msg.value.
    def parse_value(msg: KafkaSourceMessage):
        return json.loads(msg.value)

    parsed_stream = op.map("parse_value", stream.oks, parse_value)
    # op.inspect("inspect-out-data", parsed_stream)

    #######################################################
    # Group the readings by sensor type, so that we only
    # aggregate readings of the same type.
    keyed_stream = op.key_on("key_on_user", parsed_stream, lambda e: e["cc_num"])

    # Configure the `fold_window` operator to use the event time.
    # clock = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=int(21600000000)))
    clock = EventClockConfig(get_event_time,
                             wait_for_system_duration=timedelta(seconds=10))

    # And a 5 seconds tumbling window
    # windower = TumblingWindow(align_to=align_to, length=timedelta(seconds=int(60000 * 60)))
    windower = SlidingWindow(
        length=timedelta(seconds=int(20)),
        offset=timedelta(seconds=5),
        align_to=align_to,
    )

    windowed_stream = win.fold_window("add", keyed_stream, clock, windower, list, accumulate)
    # op.inspect("inspect-windowed-stream", windowed_stream)

    formatted_stream = op.map("format_event", windowed_stream, format_event)
    # op.inspect("inspect-formatted-stream", formatted_stream)

    # op.output("out", formatted_stream, StdOutSink())
    # sync to feature group topic
    fg_serialized_stream = op.map("serialize_with_key", formatted_stream,
                                  lambda x: serialize_with_key(x, feature_group))

    processed = op.map("map", fg_serialized_stream, lambda x: sink_kafka(x[0], x[1], feature_group))

    kop.output(
        "kafka-out",
        processed,
        brokers=kafka_config['bootstrap.servers'],
        topic=feature_group._online_topic_name,
        add_config=kafka_config,
    )

    return flow
