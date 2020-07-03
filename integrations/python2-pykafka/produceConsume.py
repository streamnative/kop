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

from pykafka import KafkaClient
import os
import traceback

if __name__ == '__main__':
    broker_list = os.getenv("KOP_BROKER") if os.getenv("KOP_BROKER") is not None else "localhost:9092"
    topic = os.getenv("KOP_TOPIC") if os.getenv("KOP_TOPIC") is not None else "test"
    should_produce = True if os.getenv("KOP_PRODUCE") is not None \
                                                 and os.getenv("KOP_PRODUCE") == 'true' else False
    should_consume = True if os.getenv("KOP_CONSUME") is not None \
                                                 and os.getenv("KOP_CONSUME") == 'true' else False
    limit = int(os.getenv("KOP_LIMIT")) if os.getenv("KOP_LIMIT") is not None else 10
    group_id = os.getenv("KOP_GROUPID") if os.getenv("KOP_GROUPID") is not None else 'test_kop'
    balance_consume = True if os.getenv("KOP_BALANCE_CONSUME") is not None \
                                                          and os.getenv("KOP_BALANCE_CONSUME") == 'true' else False
    use_rdkafka = True if os.getenv("KOP_USE_RDKAFKA") is not None \
                                                  and os.getenv("KOP_USE_RDKAFKA") == 'true' else False

    print broker_list
    client = KafkaClient(hosts=broker_list)
    myTopic = client.topics[topic]
    if should_produce is True and use_rdkafka is False:
        print "starting to produce\n"
        with myTopic.get_sync_producer() as producer:
            try:
                for i in range(limit):
                    producer.produce("hello kop, id: " + str(i))
                    print "hello kop, id: " + str(i)
                print "produced all messages successfully\n"
            except Exception as e:
                print "Exception: " + traceback.format_exc()
                raise e

    if should_produce is True and use_rdkafka is True:
        print "starting to produce\n"
        producer = None
        try:
            producer = myTopic.get_producer(use_rdkafka = use_rdkafka)
            for i in range(limit):
                producer.produce("hello, kop, id: " + str(i))
            print "produced all messages successfully\n"
        except Exception as e:
            print "Exception: " + traceback.format_exc()
            raise e
        finally:
            if producer is not None:
                producer.close()

    if should_consume is True and balance_consume is False:
        print "starting to consume\n"
        count = 0
        consumer = None
        try:
            consumer = myTopic.get_simple_consumer(use_rdkafka=use_rdkafka)
            for message in consumer:
                if message is not None:
                    count += 1
                    print message.offset, message.value
                    if count == limit:
                        break
        except Exception as e:
            print "Exception: " + traceback.format_exc()
            raise e
        finally:
            if consumer is not None:
                consumer.close()

    if should_consume is True and balance_consume is True:
        print "starting to consume\n"
        count = 0
        consumer = None
        try:
            balanced_consumer = myTopic.get_balanced_consumer(
                consumer_group=group_id,
                auto_commit_enable=True,
                use_rdkafka=use_rdkafka)
            for message in consumer:
                if message is not None:
                    count += 1
                    print message.offset, message.value
                    if count == limit:
                        break
        except Exception as e:
            print "Exception: " + traceback.format_exc()
            raise e
        finally:
            if consumer is not None:
                consumer.close()

