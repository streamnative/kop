#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#-*- coding:utf-8 -*-

from kafka import KafkaConsumer, KafkaProducer
import os
import traceback

if __name__ == '__main__':
    broker_list = os.getenv("KOP_BROKER") if os.getenv("KOP_BROKER") is not None else "localhost:9092"
    topic = os.getenv("KOP_TOPIC") if os.getenv("KOP_TOPIC") is not None else "test"
    should_produce = os.getenv("KOP_PRODUCE") if os.getenv("KOP_PRODUCE") is not None \
                                                 and os.getenv("KOP_PRODUCE") == 'true' else False
    should_consume = os.getenv("KOP_CONSUME") if os.getenv("KOP_CONSUME") is not None \
                                                 and os.getenv("KOP_CONSUME") == 'true' else False
    limit = int(os.getenv("KOP_LIMIT")) if os.getenv("KOP_LIMIT") is not None else 10
    group_id = os.getenv("KOP_GROUPID") if os.getenv("KOP_GROUPID") is not None else 'test_kop'

    if should_produce is True:
        print "starting to produce\n"
        producer = None
        try:
            producer = KafkaProducer(bootstrap_servers=broker_list)
            msg = "hello pulsar kop, id: "
            for x in range(0, limit):
                producer.send(topic, msg + str(x))
            print "produced all messages successfully\n"
        except Exception as e:
            print "Exception: " + traceback.format_exc()
            raise e
        finally:
            if producer is not None:
                producer.close()

    if should_consume is True:
        print "starting to consume\n"
        consumer = None
        try:
            consumer = KafkaConsumer(topic, bootstrap_servers=broker_list, group_id=group_id, auto_offset_reset='smallest')
            count = 0
            for message in consumer:
                count += 1
                print "receive message: " + str(message.value) + '\n'
                if count == limit:
                    break
            print "consumed all messages successfully\n"
        except Exception as e:
            print "Exception: " + traceback.format_exc()
            raise e
        finally:
            if consumer is not None:
                consumer.close()
