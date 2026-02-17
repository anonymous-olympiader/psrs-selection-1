"""
Генератор тестовых данных для задачи подсчёта уникальных IPv6.

Создаёт файл с заданным числом уникальных адресов и общим количеством строк.
Повторы — это тот же адрес в разных формах записи (полная/сокращённая, upper/lower).
Используется встроенный ipaddress для генерации и вариантов.
"""
import ipaddress
import random
import sys
import argparse


def rand_ipv6():
    """Случайный IPv6 через 128 случайных бит."""
    return ipaddress.IPv6Address(random.getrandbits(128))


def variants(addr):
    """Четыре варианта записи одного адреса: exploded/compressed × lower/upper."""
    ex = addr.exploded.lower()
    yield ex
    yield ex.upper()
    yield addr.compressed.lower()
    yield addr.compressed.upper()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate IPv6 addresses dataset for olympiad task'
    )
    parser.add_argument('output_file', help='Output file path')
    parser.add_argument('num_unique', type=int, help='Number of unique IPv6 addresses')
    parser.add_argument('total_size', type=int, help='Total number of lines in output')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility (default: 42)')

    args = parser.parse_args()

    # Проверка параметров
    if args.num_unique <= 0:
        print(f"Error: num_unique must be positive, got {args.num_unique}", file=sys.stderr)
        sys.exit(1)

    if args.total_size <= 0:
        print(f"Error: total_size must be positive, got {args.total_size}", file=sys.stderr)
        sys.exit(1)

    if args.total_size < args.num_unique:
        print(f"Error: total_size ({args.total_size}) cannot be less than num_unique ({args.num_unique})",
              file=sys.stderr)
        sys.exit(1)

    random.seed(args.seed)

    # Генерация уникальных адресов и вариантов их записи
    address_variants = {}
    while len(address_variants) < args.num_unique:
        a = rand_ipv6()
        if a not in address_variants:
            address_variants[a] = list(variants(a)) if args.total_size > args.num_unique else [str(a)]

    output_lines = [str(addr) for addr in address_variants.keys()]

    # Дополнение до total_size путём добавления случайных вариантов существующих адресов
    if args.total_size > len(output_lines):
        keys = list(address_variants.keys())
        while len(output_lines) < args.total_size:
            random_addr = random.choice(keys)
            random_variant = random.choice(address_variants[random_addr])
            output_lines.append(random_variant)

    random.shuffle(output_lines)
    with open(args.output_file, 'w') as f:
        f.write("\n".join(output_lines))
