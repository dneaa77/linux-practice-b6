import threading
import time

THREADS = 4
PHASES = 3

condition = threading.Condition()
waiting = 0
phase = 1


def barrier(thread_id):
    global waiting, phase

    with condition:
        current_phase = phase
        waiting += 1

        if waiting == THREADS:
            print(f"Все потоки завершили фазу {current_phase}")

            waiting = 0
            phase += 1

            condition.notify_all()
        else:
            while phase == current_phase:
                condition.wait()


def worker(thread_id):
    for current_phase in range(1, PHASES + 1):
        print(
            f"Поток {thread_id}: начал фазу {current_phase}"
        )

        time.sleep(0.1 * thread_id)

        print(
            f"Поток {thread_id}: закончил работу фазы {current_phase}"
        )

        barrier(thread_id)

    print(f"Поток {thread_id}: завершил все фазы")


def main():
    threads = [
        threading.Thread(target=worker, args=(i,))
        for i in range(1, THREADS + 1)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("Все 4 потока завершили 3 фазы.")


if __name__ == "__main__":
    main()
