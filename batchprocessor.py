import time
import json
import os
from typing import Iterable, Callable, Any, Generator, Optional
from tqdm import tqdm  # For progress tracking
from functools import wraps


class BatchProcessor:
    def __init__(
        self,
        iterable: Iterable,
        batch_size: int,
        progress: bool = False,
        save_to_file: Optional[str] = None,
        retries: int = 0,
        retry_delay: float = 1.0,
    ):
        """
        A modular batch processor with progress tracking, retries, and saving options.

        Args:
            iterable: The input iterable (e.g., list, generator).
            batch_size: The size of each batch.
            progress: Whether to show a progress bar.
            save_to_file: Path to save batch results (JSON format).
            retries: Number of retries for batch processing failures.
            retry_delay: Delay (in seconds) between retries.
        """
        self.iterable = iterable
        self.batch_size = batch_size
        self.progress = progress
        self.save_to_file = save_to_file
        self.retries = retries
        self.retry_delay = retry_delay

    def get_batches(self) -> Generator[list, None, None]:
        """Yields batches of the specified size."""
        batch = []
        for item in self.iterable:
            batch.append(item)
            if len(batch) == self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def process(self, func: Callable[[list], Any], *args, **kwargs):
        """
        Processes each batch using the provided function.

        Args:
            func: A function that processes a batch.
            *args, **kwargs: Additional arguments for the function.
        """
        results = []
        batches = self.get_batches()
        if self.progress:
            batches = tqdm(batches, desc="Processing Batches")

        for i, batch in enumerate(batches):
            success = False
            for attempt in range(self.retries + 1):
                try:
                    print(f"\nProcessing Batch {i+1}, Attempt {attempt+1}")
                    result = func(batch, *args, **kwargs)
                    results.append(result)
                    success = True
                    break  # Exit retry loop on success
                except Exception as e:
                    print(f"Error: {e}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)

            if not success:
                print(f"Batch {i+1} failed after {self.retries} retries.")

            # Save batch result to file if needed
            if self.save_to_file:
                self._save_to_file(i + 1, result)

        print("Processing complete.")
        return results

    def _save_to_file(self, batch_number: int, result: Any):
        """Saves the result of a batch to a file."""
        file_name, ext = os.path.splitext(self.save_to_file)
        batch_file = f"{file_name}_batch{batch_number}{ext}"
        with open(batch_file, "w") as f:
            json.dump(result, f, indent=4)
        print(f"Batch {batch_number} result saved to {batch_file}")


# Decorator for batch processing
def batch_process(
    batch_size: int,
    progress: bool = False,
    save_to_file: Optional[str] = None,
    retries: int = 0,
    retry_delay: float = 1.0,
):
    """
    A decorator for batch processing with retries, progress tracking, and saving options.

    Args:
        batch_size: The size of each batch.
        progress: Whether to show a progress bar.
        save_to_file: Path to save batch results (JSON format).
        retries: Number of retries for batch processing failures.
        retry_delay: Delay (in seconds) between retries.
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(iterable: Iterable, *args, **kwargs):
            processor = BatchProcessor(
                iterable,
                batch_size,
                progress=progress,
                save_to_file=save_to_file,
                retries=retries,
                retry_delay=retry_delay,
            )
            return processor.process(func, *args, **kwargs)

        return wrapper

    return decorator
