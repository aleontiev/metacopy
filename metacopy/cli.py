#!?user/bin/env python

from cleo import Application
from .command import Copy


def main():
    copy = Copy()
    application = Application()
    application.add(copy)
    application.run()


if __name__ == '__main__':
    main()
