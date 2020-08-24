#!?user/bin/env python

from cleo import Application
from .command import Copy, CopyQuestion, CopyCollection


def main():
    copy = Copy()
    copy_question = CopyQuestion()
    copy_collection = CopyCollection()

    application = Application()
    application.add(copy)
    application.add(copy_question)
    application.add(copy_collection)
    application.run()


if __name__ == '__main__':
    main()
