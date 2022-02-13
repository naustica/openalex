import gzip
import ujson
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import os


works_directory = '/scratch/users/haupka/openalex-snapshot/data/works'
output_directory = '/scratch/users/haupka/works'
error_log_directory = '/scratch/users/haupka'


def transform_file(input_file_path: str, output_file_path: str):
    new_data = []

    with gzip.open(input_file_path, 'r') as file:
        for line in file:
            try: 
                new_item = ujson.loads(item)
                if isinstance(new_item, dict):
                    new_data.append(new_item)
            except:
                with open(error_log_directory + '/' + 'error_log.txt', 'a') as error_log_file:
                    error_log_file.write(item.decode('utf-8'))
    
    with gzip.open(output_file_path, 'w') as output_file:
        result = [ujson.dumps(record,
                              ensure_ascii=False,
                              escape_forward_slashes=False).encode('utf-8') for record in new_data]
        for i in result:
            output_file.write(i + bytes('\n', encoding='utf8'))


def transform_snapshot(max_workers: int = cpu_count()):

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for directory in os.listdir(works_directory):
            if os.path.isdir(works_directory + '/' + directory):
                os.makedirs(output_directory + '/' + directory, exist_ok=True)
                for input_file in os.listdir(works_directory + '/' + directory):
                    output_file_path = os.path.join(output_directory + '/' + directory, os.path.basename(input_file))
                    future = executor.submit(transform_file,
                                             input_file_path=works_directory + '/' + directory + '/' + input_file,
                                             output_file_path=output_file_path)
                    futures.append(future)

        for future in as_completed(futures):
            future.result()


transform_snapshot()
