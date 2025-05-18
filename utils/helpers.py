from collections import deque, namedtuple


def keep_first_last_curly_brackets(text: str) -> str:
    """Return substring from the first "{" to the last "}" (both inclusive)."""

    left, right = text.find("{"), text.rfind("}")
    return text[left: right + 1] if left != -1 and right != -1 else text


RetryItem = namedtuple('RetryItem', ['id', 'attempts'])


class RetryQueue:
    def __init__(self,
                 max_size: int = 512,
                 max_retry_count: int = 3,
                 backoff_factor: int = 1):
        self.max_size = max_size
        self.max_retry_count = max_retry_count
        self.backoff_factor = backoff_factor

        self.queue = deque[RetryItem](maxlen=self.max_size)

    def enqueue(self, item: RetryItem) -> None:
        self.queue.append(item)

    def dequeue(self) -> RetryItem:
        return self.queue.popleft()

    def is_empty(self) -> bool:
        return len(self.queue) == 0

    def backoff(self, attempt: int) -> int:
        return self.backoff_factor * (2 ** (attempt - 1))

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_retry_count

    def __len__(self) -> int:
        return len(self.queue)

    def __repr__(self):
        return (f"RetryQueue(max_size={self.max_size}, "
                f"max_retry_count={self.max_retry_count}, "
                f"backoff_factor={self.backoff_factor})")
