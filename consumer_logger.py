from consumer import run


def log(msg: str):
    print(msg)


if __name__ == '__main__':
    run(group_id='logger', process_message=log)