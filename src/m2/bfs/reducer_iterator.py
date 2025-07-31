#!/usr/bin/env python3
import sys


def main():
    last_line = None
    for line in sys.stdin:
        line = line.strip()
        if line != last_line:
            print(line)
            last_line = line


if __name__ == "__main__":
    main()
