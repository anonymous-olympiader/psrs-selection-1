#!/usr/bin/env python3
"""
Подсчёт уникальных IPv6-адресов в текстовом файле.

Режимы работы:
- basic: весь файл в памяти (set), для объёма до ~10^6 строк
- optimized: разбиение по хешу на партиции, потоковое чтение, для больших файлов
  при ограничении RAM ~1 ГБ

Выбор режима автоматически по размеру файла (порог 50 МБ) или вручную через флаги.
"""
import argparse
import hashlib
import ipaddress
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed


# Порог 50 МБ (~10^6 строк) для переключения на режим партиций
MEMORY_MODE_THRESHOLD = 50 * 1024 * 1024
NUM_PARTITIONS = 4096
CHUNK_WRITE_SIZE = 8 * 1024 * 1024  # Размер буфера при записи партиций


def ipv6_to_canonical(addr_str: str) -> str:
    """Приводит IPv6 к канонической форме: 8 групп по 4 hex, lowercase, разделитель ':'
    Пример: 2001:db0::30 -> 2001:0db0:0000:0000:0000:0000:0000:0030
    """
    addr = ipaddress.IPv6Address(addr_str.strip())
    return addr.exploded.lower()


def count_unique_in_partition(partition_path: str) -> int:
    """Подсчёт уникальных строк в одном файле партиции.
    Вызывается воркерами при параллельной обработке. Строки уже в канонической форме.
    """
    unique = set()
    with open(partition_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if line:
                unique.add(line)
    return len(unique)


def count_unique_basic(input_path: str) -> int:
    """Базовый режим: построчное чтение файла, нормализация адресов, хранение в set.
    Предназначен для файлов до ~10^6 строк, помещающихся в оперативную память.
    """
    unique = set()
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if line:
                canonical = ipv6_to_canonical(line)
                unique.add(canonical)
    return len(unique)


def count_unique_optimized(input_path: str, temp_dir: str, num_workers: int = 0) -> int:
    """Оптимизированный режим для больших файлов.

    Алгоритм:
    1. Потоковое чтение, нормализация, разбиение по MD5-хешу в NUM_PARTITIONS файлов
    2. Параллельный подсчёт уникальных в каждой партиции (ProcessPoolExecutor)
    3. Сумма результатов

    В памяти одновременно только одна партиция — соблюдается лимит RAM.
    """
    num_workers = num_workers or max(1, os.cpu_count() - 1)
    partition_paths = [
        os.path.join(temp_dir, f"part_{i:04d}.txt")
        for i in range(NUM_PARTITIONS)
    ]
    for p in partition_paths:
        open(p, 'w').close()

    # Фаза 1: разбиение по партициям
    partition_files = [open(p, 'a', encoding='utf-8') for p in partition_paths]
    try:
        buffer = [[] for _ in range(NUM_PARTITIONS)]
        buffer_size = 0

        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                canonical = ipv6_to_canonical(line)
                # hash() в Python не детерминирован между запусками; MD5 даёт стабильное распределение
                h = int(hashlib.md5(canonical.encode()).hexdigest()[:8], 16)
                idx = h % NUM_PARTITIONS
                buffer[idx].append(canonical + '\n')
                buffer_size += len(canonical) + 1

                if buffer_size >= CHUNK_WRITE_SIZE:
                    for i, fh in enumerate(partition_files):
                        if buffer[i]:
                            fh.writelines(buffer[i])
                            buffer[i] = []
                    buffer_size = 0

        for i, fh in enumerate(partition_files):
            if buffer[i]:
                fh.writelines(buffer[i])
    finally:
        for fh in partition_files:
            fh.close()

    # Фаза 2: параллельный подсчёт уникальных по партициям
    total = 0
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(count_unique_in_partition, p): p
            for p in partition_paths if os.path.getsize(p) > 0
        }
        for future in as_completed(futures):
            total += future.result()

    return total


def main():
    """Точка входа: разбор аргументов, выбор режима, запись результата."""
    parser = argparse.ArgumentParser(
        description='Подсчёт уникальных IPv6-адресов во входном файле'
    )
    parser.add_argument('input_file', help='Путь к входному файлу с IPv6-адресами')
    parser.add_argument('output_file', help='Путь к выходному файлу с результатом')
    parser.add_argument('--optimized', action='store_true',
                        help='Всегда использовать режим партиционирования')
    parser.add_argument('--basic', action='store_true',
                        help='Всегда использовать in-memory режим')
    parser.add_argument('--workers', type=int, default=0,
                        help='Количество рабочих процессов (0 — авто)')
    args = parser.parse_args()

    input_path = args.input_file
    output_path = args.output_file

    if not os.path.isfile(input_path):
        raise SystemExit(f"Ошибка: файл не найден: {input_path}")

    if args.basic:
        count = count_unique_basic(input_path)
    elif args.optimized or os.path.getsize(input_path) > MEMORY_MODE_THRESHOLD:
        with tempfile.TemporaryDirectory(prefix='ipv6_count_') as tmpdir:
            count = count_unique_optimized(
                input_path,
                tmpdir,
                num_workers=args.workers,
            )
    else:
        count = count_unique_basic(input_path)

    with open(output_path, 'w') as f:
        f.write(str(count))


if __name__ == '__main__':
    main()
