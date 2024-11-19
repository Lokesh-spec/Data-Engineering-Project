import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # Example input: Event data with timestamps
    input_data = [
        ('user1', 1, '2024-11-19T10:00:00'),
        ('user2', 2, '2024-11-19T10:01:00'),
        ('user1', 3, '2024-11-19T10:03:00'),
        ('user2', 4, '2024-11-19T20:07:00'),
    ]

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Create Events" >> beam.Create(input_data)
            | "Add Timestamps" >> beam.Map(lambda x: beam.window.TimestampedValue((x[0], x[1]), int(x[2][11:13])*60 + int(x[2][14:16])))  # Add timestamp
            | "print" >> beam.Map(print)
            # | "Apply Fixed Window" >> beam.WindowInto(beam.window.FixedWindows(5 * 60))  # 5-minute windows
            # | "Sum Values Per Key" >> beam.CombinePerKey(sum)  # Aggregate within each window
            # | "Print Results" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()
