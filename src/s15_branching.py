from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.functions import (
    ProcessFunction,
    CoProcessFunction,
    CoMapFunction,
    CoFlatMapFunction,
)

if __name__ == "__main__":
    # fmt: off
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ## Union
    print(">>>> Union <<<<")
    ds1 = env.from_collection(collection=[1, 2])
    ds2 = env.from_collection(collection=[3, 4])
    ds1.union(ds2).print()
    env.execute()

    ## Connect
    ## connects two data streams retaining their types. Connect allowing for shared state between the two streams.
    ## A connect operation is more general then a join operation. 
    ## Connect ensures that two streams (keyed or unkeyed) meet at the same location (at the same parallel instance within a CoXXXFunction).
    # One stream could be a control stream that manipulates the behavior applied to the other stream. 
    # For example, you could stream-in new machine learning models or other business rules.
    # Alternatively, you can use the property of two streams that are keyed and meet at the same location for joining. 
    # Flink provides some predefined join operators. 
    # However, joining of data streams often depends on different use case-specific behaviors such as "How long do you want to wait for the other key to arrive?",
    # "Do you only look for one matching pair or more?", 
    # or "Are there late elements that need special treatment if no matching record arrives or the other matching record is not stored in state anymore?". 
    # A connect() allows you to implement your own joining logic if needed. 
    # The data Artisans training here explains one example of connect for joining.


    ds1 = env.from_collection(collection=[1, 2])
    ds2 = env.from_collection(collection=[3, 4])
    ds1.connect(ds2)

    ## CoProcessFunction
    print(">>>> CoProcessFunction <<<<")
    class MyCoProcessFunction(CoProcessFunction):
        def process_element1(self, value, ctx: "CoProcessFunction.Context"):
            return Row(value * 2)

        def process_element2(self, value, ctx: "CoProcessFunction.Context"):
            return Row(value * 3)

    ds1 = env.from_collection(collection=[1, 2])
    ds2 = env.from_collection(collection=[3, 4])
    connected = ds1.connect(ds2)
    connected.process(MyCoProcessFunction()).print()
    env.execute()

    ## CoMapFunction
    print(">>>> CoMapFunction <<<<")
    class MyCoMapFunction(CoMapFunction):
        def map1(self, value):
            return value[0] + 1, value[1]

        def map2(self, value):
            return value[0], value[1] + "flink"

    ds1 = env.from_collection(collection=[(1, "data streaming")])
    ds2 = env.from_collection(collection=[(2, "py")])
    connected = ds1.connect(ds2)
    connected.map(MyCoMapFunction()).print()
    env.execute()

    ## CoFlatMap
    print(">>>> CoFlatMapFunction <<<<")
    class MyCoFlatMapFunction(CoFlatMapFunction):
        def flat_map1(self, value):
            splits = value.split(" ")
            for sp in splits:
                yield sp, len(sp)

        def flat_map2(self, value):
            splits = value.split(" ")
            for sp in splits:
                yield sp.upper(), len(sp)

    ds1 = env.from_collection(collection=["hello apache flink", "streaming compute"])
    ds2 = env.from_collection(collection=["data stream api", "table api", "sql api"])
    connected = ds1.connect(ds2)
    connected.flat_map(MyCoFlatMapFunction()).print()
    env.execute()

    ## Splitting Streams
    print(">>>> Splitting Streams <<<<")
    ds = env.from_collection(collection=range(10))
    filter1 = ds.filter(lambda e: e > 0 and e % 2 == 0)
    filter2 = ds.filter(lambda e: e % 2 != 0)
    map = filter1.map(lambda e: e * 2)
    filter2.print()
    map.union(filter2).print()
    env.execute()

    ## Side Outputs
    # Emitting data to a side output is possible from the following functions:
    #     ProcessFunction
    #     KeyedProcessFunction
    #     CoProcessFunction
    #     KeyedCoProcessFunction
    #     ProcessWindowFunction
    #     ProcessAllWindowFunction
    print(">>>> Side Outputs <<<<")
    output_tag = OutputTag("side-output", Types.STRING())

    class MyProcessFunction(ProcessFunction):
        def process_element(self, value, ctx: "ProcessFunction.Context"):
            # emit to regular output
            yield value
            # emit to side output
            yield output_tag, "sideoutput-" + str(value)

    ds = env.from_collection(collection=range(5))
    main_data_stream = ds.process(MyProcessFunction(), Types.INT())
    side_output_stream = main_data_stream.get_side_output(output_tag)

    print("==== main data stream")
    main_data_stream.print()
    env.execute()

    print("==== side output stream")
    side_output_stream.print()
    env.execute()
