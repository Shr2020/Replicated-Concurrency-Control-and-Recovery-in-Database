from enum import Enum

class TransactionStates(Enum):
    BLOCK = 0                     # transaction couldn’t proceed because of chance of deadlock
    RUNNING  = 1                  # transaction has needed locks and its running
    ABORTED  = 2                  # aborted 
    COMMITED = 3                  # transaction completed
    WAITING  = 4                  # waiting transaction for a lock
    START = 5                     # transaction started
    TO_BE_ABORTED = 6
