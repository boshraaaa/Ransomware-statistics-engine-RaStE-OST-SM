from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

env.from_collection([1, 2, 3, 4]).map(lambda x: x * 2).print()

env.execute("Simple Flink Job")
