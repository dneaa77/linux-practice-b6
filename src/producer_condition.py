import threading
from collections import Counter

BUFFER_SIZE = 5
TOTAL_ITEMS = 1000
PRODUCERS = 3
CONSUMERS = 2

buffer = [None] * BUFFER_SIZE
write_pos = 0
read_pos = 0
count = 0

condition = threading.Condition()

produced = []
consumed = []
errors = []


def producer(producer_id):
    global write_pos, count

    for item in range(producer_id, TOTAL_ITEMS, PRODUCERS):
        with condition:
            while count == BUFFER_SIZE:
                condition.wait()

            if buffer[write_pos] is not None:
                errors.append(f"Переполнение: позиция {write_pos}")

            buffer[write_pos] = item
            write_pos = (write_pos + 1) % BUFFER_SIZE
            count += 1
            produced.append(item)

            condition.notify_all()


def consumer():
    global read_pos, count

    while True:
        with condition:
            while count == 0:
                condition.wait()

            item = buffer[read_pos]
            buffer[read_pos] = None
            read_pos = (read_pos + 1) % BUFFER_SIZE
            count -= 1

            condition.notify_all()

        if item == -1:
            break

        consumed.append(item)


def main():
    global write_pos, count

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

    # Отправляем завершающий сигнал каждому потребителю.
    for _ in range(CONSUMERS):
        with condition:
            while count == BUFFER_SIZE:
                condition.wait()

            buffer[write_pos] = -1
            write_pos = (write_pos + 1) % BUFFER_SIZE
            count += 1

            condition.notify_all()

    for thread in consumers:
        thread.join()

    produced_counts = Counter(produced)
    consumed_counts = Counter(consumed)

    print("Версия: мьютекс + условная переменная")
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

