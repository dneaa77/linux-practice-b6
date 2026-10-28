import threading
import time

condition = threading.Condition()
ready = False


def consumer():
    global ready

    with condition:
        if not ready:
            print("Потребитель: ожидает сигнал")
            condition.wait()

        print("Потребитель: продолжает работу")
        print("ready =", ready)


def producer():
    global ready

    time.sleep(0.2)

    with condition:
        ready = True
        print("Производитель: ready = True")
        condition.notify()


def main():
    c = threading.Thread(target=consumer)
    p = threading.Thread(target=producer)

    c.start()
    p.start()

    c.join()
    p.join()

    print("Демонстрация завершена.")


if __name__ == "__main__":
    main()
