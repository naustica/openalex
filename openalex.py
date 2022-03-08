import gzip
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import os
import uuid


works_directory = '/scratch/users/haupka/openalex-snapshot/data/works'
output_directory = '/scratch/users/haupka/works'
error_log_directory = '/scratch/users/haupka'


def transform_file(input_file_path: str, output_file_path: str):
    new_data = []

    with gzip.open(input_file_path, 'r') as file:
        for i, line in enumerate(file, start=0):

            new_item = json.loads(line)
            if isinstance(new_item, dict):
                new_data.append(new_item)

            # reduce array length -> out of memory error
            if len(new_data) == 10000:
                write_file(new_data, output_file_path)
                new_data = []

        write_file(new_data, output_file_path)


def write_file(data, output_file_path: str):

    with gzip.open(output_file_path + '/' + str(uuid.uuid4()) + '.gz', 'w') as output_file:
        result = [json.dumps(record, ensure_ascii=False).encode('utf-8') for record in data]
        for line in result:
            output_file.write(line + bytes('\n', encoding='utf8'))


def transform_snapshot(max_workers: int = cpu_count()):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for directory in os.listdir(works_directory):
            if os.path.isdir(works_directory + '/' + directory):
                os.makedirs(output_directory + '/' + directory, exist_ok=True)
                for input_file in os.listdir(works_directory + '/' + directory):
                    output_file_path = os.path.join(output_directory + '/' + directory)
                    future = executor.submit(transform_file,
                                             input_file_path=works_directory + '/' + directory + '/' + input_file,
                                             output_file_path=output_file_path)
                    futures.append(future)

        for future in as_completed(futures):
            future.result()


transform_snapshot()
