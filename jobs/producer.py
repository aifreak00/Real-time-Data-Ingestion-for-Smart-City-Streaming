import uuid
import simplejson as json

def json_serializer(obj):
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.name__} is not JSON serializable")

def delivery_report(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer,topic,data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data,default=json_serializer).encode('utf-8'),
        on_delivery = delivery_report
    )
    producer.flush()
