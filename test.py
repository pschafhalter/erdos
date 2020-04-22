import erdos
from collections import deque
import sys
import time


class SendOp(erdos.Operator):
    def __init__(self, write_stream, must_sleep):
        self.write_stream = write_stream
        self.must_sleep = must_sleep

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        while True:
            timestamp = erdos.Timestamp(coordinates=[count])
            msg = erdos.Message(timestamp, count)
            #print("SendOp: sending {msg}".format(msg=msg))
            # if self.must_sleep:
            #     time.sleep(0.02)
            self.write_stream.send(msg)
            self.write_stream.send(erdos.WatermarkMessage(timestamp))
            count += 1
            time.sleep(0.1)


class MapOp(erdos.Operator):
    def __init__(self, input_stream1, input_stream2, output_stream):
        input_stream1.add_callback(self.on_msg1)
        input_stream2.add_callback(self.on_msg2)
        erdos.add_watermark_callback([input_stream1], [output_stream],
                                     self.on_watermark)
        self.q1 = deque()
        self.q2 = deque()

    @staticmethod
    def connect(input_stream1, input_stream2):
        return [erdos.WriteStream()]

    def on_msg1(self, msg):
        self.q1.append(msg)

    def on_msg2(self, msg):
        self.q2.append(msg)

    def on_watermark(self, timestamp, output_stream):
        print(f"sending message {timestamp}")
        msg1 = self.q1.popleft()
        #        msg2 = self.q2.popleft()
        #        assert msg1.timestamp == msg2.timestamp
        output_stream.send(msg1)


class LastOp(erdos.Operator):
    def __init__(self, input_stream1, input_stream2):
        erdos.add_watermark_callback([input_stream1, input_stream2], [],
                                     self.on_watermark)
        input_stream1.add_callback(self.on_msg)
        self.q1 = deque()

    @staticmethod
    def connect(input_stream1, input_stream2):
        return []

    def on_msg(self, msg):
        time.sleep(0.1)
        self.q1.append(msg)

    def on_watermark(self, timestamp):
        msg = self.q1.popleft()
        print(f"Watermark {timestamp}; msg {msg.timestamp}")
        assert msg.timestamp == timestamp


def main():
    (is1, ) = erdos.connect(SendOp, erdos.OperatorConfig(), [], False)
    (is2, ) = erdos.connect(SendOp, erdos.OperatorConfig(), [], True)
    (os, ) = erdos.connect(MapOp, erdos.OperatorConfig(), [is1, is2])
    erdos.connect(LastOp, erdos.OperatorConfig(), [os, is2])
    erdos.run(graph_filename="test.gv")


if __name__ == "__main__":
    main()
