import threading
from collections import Counter

BUFFER_SIZE = 5
TOTAL_ITEMS = 1000
PRODUCERS = 3
CONSUMERS = 2

buffer = [None] * BUFFER_SIZE
write_pos = 0
read_pos = 0

empty = threading.Semaphore(BUFFER_SIZE)
full = threading.Semaphore(0)
mutex = threading.Lock()

produced = []
consumed = []
errors = []


def producer(producer_id):
    global write_pos

    for item in range(producer_id, TOTAL_ITEMS, PRODUCERS):
        empty.acquire()

        with mutex:
            if buffer[write_pos] is not None:
                errors.append(f"Переполнение: позиция {write_pos}")

            buffer[write_pos] = item
            write_pos = (write_pos + 1) % BUFFER_SIZE
            produced.append(item)

        full.release()


def consumer():
    global read_pos

    while True:
        full.acquire()

        with mutex:
            item = buffer[read_pos]

            if item is None:
                errors.append(f"Чтение пустой ячейки: {read_pos}")
            else:
                buffer[read_pos] = None

            read_pos = (read_pos + 1) % BUFFER_SIZE

            if item != -1:
                consumed.append(item)

        empty.release()

        if item == -1:
            break


def main():
    producers = [
        threading.Thread(target=producer, args=(i,))
        for i in range(PRODUCERS)
    ]

    consumers = [
        threading.Thread(target=consumer)
        for _ in range(CONSUMERS)
    ]

    for thread in consumers:
        thread.start()

    for thread in producers:
        thread.start()

    for thread in producers:
        thread.join()

    # Отправляем по одному завершающему сигналу каждому потребителю.
    for _ in range(CONSUMERS):
        empty.acquire()

        global write_pos
        with mutex:
            if buffer[write_pos] is not None:
                errors.append(f"Переполнение при завершении: {write_pos}")

            buffer[write_pos] = -1
            write_pos = (write_pos + 1) % BUFFER_SIZE

        full.release()

    for thread in consumers:
        thread.join()

    produced_counts = Counter(produced)
    consumed_counts = Counter(consumed)

    print("Версия: семафоры + мьютекс")
    print(f"Произведено: {len(produced)}")
    print(f"Потреблено: {len(consumed)}")
    print(f"Уникальных элементов: {len(consumed_counts)}")
    print(f"Нарушений: {len(errors)}")

    for error in errors:
        print("НАРУШЕНИЕ:", error)

    if (
        len(produced) == TOTAL_ITEMS
        and len(consumed) == TOTAL_ITEMS
        and produced_counts == consumed_counts
        and not errors
    ):
        print("РЕЗУЛЬТАТ: PASS")
    else:
        print("РЕЗУЛЬТАТ: FAIL")


if __name__ == "__main__":
    main()
