from batchprocessor import BatchProcessor
import random

# Example function to process a batch
def process_batch(batch):
    print(f"Processing: {batch}")
    # if random.choice([True, False]):  # Simulate random failure
    #     raise ValueError("Random failure occurred!")
    result = []
    for i in list(batch):
        result.append(i + 1)
    return {"processed": result, "status": "success"}

# Input data
items = range(1, 23)

# Initialize the BatchProcessor
processor = BatchProcessor(
    iterable=items,
    batch_size=5,
    progress=True,
    save_to_file="output.json",
    retries=2,
    retry_delay=1.0,
)

# Process the batches
results = processor.process(process_batch)
print("Final Results:", results)

from batchprocessor import batch_processor_decorator
import random

# Decorate the function for batch processing
@batch_processor_decorator(
    batch_size=5,
    progress=True,
    save_to_file="output_decorator.json",
    retries=2,
    retry_delay=1.0,
)
def process_batch(batch):
    print(f"Processing: {batch}")
    # if random.choice([True, False]):  # Simulate random failure
    #     raise ValueError("Random failure occurred!")
    return {"processed": batch, "status": "success"}

# Input data
items = range(1, 23)

# Call the decorated function
process_batch(items)
