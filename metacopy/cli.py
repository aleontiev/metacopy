#!?user/bin/env python

from cleo import Application
from .command import Copy, CopyQuestion


def main():
    copy = Copy()
    copy_question = CopyQuestion()

    application = Application()
    application.add(copy)
    application.add(copy_question)
    application.run()


if __name__ == '__main__':
    main()
