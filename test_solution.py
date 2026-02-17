#!/usr/bin/env python3
"""
Тесты для count_unique_ipv6.

Проверки:
- example_input.txt: пример из задания (5 строк -> 4 уникальных)
- малый файл: basic и auto режимы
- средний файл: optimized режим
"""
import os
import subprocess
import sys


def run_count(input_path: str, output_path: str, args: list = None) -> int:
    """Запуск count_unique_ipv6, возврат числа из output-файла."""
    cmd = [sys.executable, 'count_unique_ipv6.py', input_path, output_path]
    if args:
        cmd.extend(args)
    subprocess.run(cmd, check=True)
    with open(output_path) as f:
        return int(f.read().strip())


def test_example():
    """Пример из задания: 2001:0DB0... и 2001:db0::30 — один адрес."""
    result = run_count('example_input.txt', 'test_output.txt')
    assert result == 4, f"Ожидалось 4, получено {result}"
    print("[OK] Пример из задания: 4 уникальных адреса")


def test_generated_small():
    """100 уникальных адресов, 500 строк (с повторами в разных формах)."""
    subprocess.run([
        sys.executable, 'generate_ipv6_data.py', 'test_small.txt', '100', '500'
    ], check=True)
    result_basic = run_count('test_small.txt', 'test_out_basic.txt', ['--basic'])
    result_auto = run_count('test_small.txt', 'test_out_auto.txt')
    assert result_basic == 100
    assert result_auto == 100
    print("[OK] Малый файл (500 строк, 100 уникальных): оба режима корректны")


def test_generated_optimized():
    """Проверка режима партиций на 5k уникальных, 25k строк."""
    subprocess.run([
        sys.executable, 'generate_ipv6_data.py', 'test_medium.txt', '5000', '25000'
    ], check=True)
    result = run_count('test_medium.txt', 'test_out_opt.txt', ['--optimized'])
    assert result == 5000
    print("[OK] Оптимизированный режим: 5000 уникальных")


def cleanup():
    """Удаление временных файлов после тестов."""
    for f in ['test_output.txt', 'test_small.txt', 'test_out_basic.txt', 
              'test_out_auto.txt', 'test_medium.txt', 'test_out_opt.txt']:
        if os.path.exists(f):
            os.remove(f)


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    try:
        test_example()
        test_generated_small()
        test_generated_optimized()
        print("\nВсе тесты пройдены.")
    finally:
        cleanup()
