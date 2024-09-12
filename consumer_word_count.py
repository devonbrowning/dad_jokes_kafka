from consumer import run

n_jokes = 0
total_word_count = 0


def output_wordcount(msg: str):
    global n_jokes
    global total_word_count
    n_jokes += 1
    word_count = len(msg.split(' '))
    total_word_count += word_count
    if n_jokes % 5 == 0:
        print(f'Average word count: {total_word_count / n_jokes}')


if __name__ == '__main__':
    run(group_id='word_count', process_message=output_wordcount)