import gzip
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import os
import uuid


input_directory = '/scratch/users/haupka/openalex-snapshot/data/works'
output_directory = '/scratch/users/haupka/works'


def transform_file(input_file_path: str, output_file_path: str):
    new_data = []

    with gzip.open(input_file_path, 'r') as file:
        for line in file:

            new_item = json.loads(line)
            if isinstance(new_item, dict):
                doi = new_item.get('doi')
                inverted_index = new_item.get('abstract_inverted_index')
                mesh = new_item.get('mesh')
                related_works = new_item.get('related_works')
                
                new_item['has_abstract'] = bool(inverted_index)

                if doi:
                    new_item['doi'] = doi.lstrip('https://doi.org/')
                if inverted_index:
                    new_item.pop('abstract_inverted_index')
                if mesh:
                    new_item.pop('mesh')
                if related_works:
                    new_item.pop('related_works')
                
                new_data.append(new_item)

            # fix out of memory error
            if len(new_data) == 10000:
                write_file(new_data, output_file_path)
                new_data.clear()

        write_file(new_data, output_file_path)


def write_file(data, output_file_path: str):

    with gzip.open(output_file_path + '/' + str(uuid.uuid4()) + '.gz', 'w') as output_file:
        result = [json.dumps(record, ensure_ascii=False).encode('utf-8') for record in data]
        for line in result:
            output_file.write(line + bytes('\n', encoding='utf8'))


def transform_snapshot(max_workers: int = cpu_count()):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for directory in os.listdir(input_directory):
            if os.path.isdir(input_directory + '/' + directory):
                os.makedirs(output_directory + '/' + directory, exist_ok=True)
                for input_file in os.listdir(input_directory + '/' + directory):
                    output_file_path = os.path.join(output_directory + '/' + directory)
                    future = executor.submit(transform_file,
                                             input_file_path=input_directory + '/' + directory + '/' + input_file,
                                             output_file_path=output_file_path)
                    futures.append(future)

        for future in as_completed(futures):
            future.result()


transform_snapshot()
